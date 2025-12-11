# ============================================================
#  JDM CASH NOW – Sistema de Préstamos Multi-Rol (PostgreSQL)
#  Funciones:
#   - Pago de CAPITAL, INTERÉS o CUOTA (capital+interés)
#   - Si paga capital → reduce remaining automático
#   - Si se paga el TOTAL (capital + intereses) → préstamo se cierra (status='cerrado')
#   - Cobradores aislados (no ven clientes de otros)
#   - Admin reasigna clientes entre cobradores
#   - Admin/Supervisor puede mover un solo cliente de cobrador
#   - Compatible con Flask 3 (sin before_first_request)
#   - Tema Claro / Oscuro con botón de cambio
#   - Frecuencia: diario / semanal / quincenal / mensual
#   - Atrasos aproximados según frecuencia e intereses
#   - Registro de efectivo entregado por trabajador (Gastos de ruta)
#   - Enviar factura al cliente por WhatsApp / SMS
# ============================================================

from __future__ import annotations

from flask import (
    Flask, request, redirect, url_for, Response,
    render_template_string, session, flash, get_flashed_messages
)
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps

import psycopg2
import psycopg2.extras

from datetime import datetime, date
import os
import secrets
from urllib.parse import quote_plus

# =============================
# CONFIGURACIÓN PRINCIPAL
# =============================

APP_BRAND = "JDM Cash Now"

# PIN para borrar / cambiar contraseñas
ADMIN_PIN = os.getenv("ADMIN_PIN", "5555")

# Roles disponibles
ROLES = ("admin", "supervisor", "cobrador")

# MONEDA
CURRENCY = "RD$"

# WhatsApp SOS / recuperación
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "3128565688")

# URL de la base de datos (Render)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ ERROR: Falta la DATABASE_URL en Render → Environment.")

# Crear app Flask
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(16))


# =============================
# CONEXIÓN A BASE DE DATOS
# =============================

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        cursor_factory=psycopg2.extras.RealDictCursor
    )


# =============================
# FORMATEO DE DINERO
# =============================

def fmt_money(val):
    try:
        v = float(val or 0)
    except (TypeError, ValueError):
        v = 0.0
    return f"{CURRENCY} {v:.2f}"


def get_theme():
    """Devuelve 'light' o 'dark' según lo guardado en sesión."""
    return session.get("theme", "light")


# =============================
# CREACIÓN / RESET DE TABLAS
# =============================

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    try:
        # ---- Tabla usuarios ----
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role VARCHAR(20) NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """)

        # ---- Tabla clientes ----
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100),
                phone VARCHAR(50),
                address TEXT,
                document_id VARCHAR(100),
                route VARCHAR(100),
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        # ---- Tabla préstamos ----
        cur.execute("""
            CREATE TABLE IF NOT EXISTS loans (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                amount NUMERIC(12,2) NOT NULL,
                rate NUMERIC(5,2) NOT NULL DEFAULT 0,
                frequency VARCHAR(20) NOT NULL,
                start_date DATE NOT NULL,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                remaining NUMERIC(12,2),
                total_interest_paid NUMERIC(12,2),
                status VARCHAR(20),
                term_count INTEGER,
                end_date DATE
            );
        """)

        # ---- Tabla pagos ----
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                loan_id INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
                amount NUMERIC(12,2) NOT NULL,
                type VARCHAR(20),
                note TEXT,
                date DATE NOT NULL,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL
            );
        """)

        # ---- Tabla auditoría ----
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                action VARCHAR(255) NOT NULL,
                detail TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        # ---- Tabla efectivo entregado (Gastos de ruta) ----
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cash_reports (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                date DATE NOT NULL,
                amount NUMERIC(12,2) NOT NULL,
                note TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        # ✅ Asegurar columnas nuevas en loans (para BD viejas)
        cur.execute("""
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS remaining NUMERIC(12,2);
        """)
        cur.execute("""
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS total_interest_paid NUMERIC(12,2) DEFAULT 0;
        """)
        cur.execute("""
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'activo';
        """)
        cur.execute("""
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS term_count INTEGER;
        """)
        cur.execute("""
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS end_date DATE;
        """)

        # ✅ Asegurar columna ruta en clientes
        cur.execute("""
            ALTER TABLE clients
            ADD COLUMN IF NOT EXISTS route VARCHAR(100);
        """)

        # ✅ Inicializar valores nulos si la tabla ya existía
        cur.execute("""
            UPDATE loans
            SET remaining = amount
            WHERE remaining IS NULL;
        """)
        cur.execute("""
            UPDATE loans
            SET total_interest_paid = 0
            WHERE total_interest_paid IS NULL;
        """)
        cur.execute("""
            UPDATE loans
            SET status = 'activo'
            WHERE status IS NULL OR status = '';
        """)

        # Crear admin si no existe
        cur.execute("SELECT COUNT(*) AS c FROM users;")
        if cur.fetchone()["c"] == 0:
            cur.execute("""
                INSERT INTO users (username, password_hash, role, created_at)
                VALUES (%s, %s, %s, %s)
            """, (
                "admin",
                generate_password_hash("admin"),
                "admin",
                datetime.utcnow(),
            ))
            print("✔ Usuario admin creado (user=admin, pass=admin)")

        conn.commit()
        print("✔ Base de datos inicializada correctamente")
    finally:
        cur.close()
        conn.close()


# ============================================================
#  PARTE 2 — Usuario actual, roles, auditoría, layout y login
# ============================================================

def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = %s;", (uid,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("Debe iniciar sesión primero.", "warning")
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def role_required(*allowed_roles):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user or user["role"] not in allowed_roles:
                flash("No tiene permiso para acceder aquí.", "danger")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def admin_required(fn):
    return role_required("admin")(fn)


def log_action(user_id, action, detail=""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO audit_log (user_id, action, detail)
        VALUES (%s, %s, %s)
    """, (user_id, action, detail))
    conn.commit()
    cur.close()
    conn.close()


# =============================
# ESTILO BASE (tema claro + oscuro)
# =============================

BASE_STYLE = """
<style>
:root {
  --green-50: #ecfdf3;
  --green-100: #dcfce7;
  --green-200: #bbf7d0;
  --green-600: #16a34a;
  --green-700: #15803d;
  --green-800: #166534;
  --red-600: #dc2626;
  --slate-800: #0f172a;
  --slate-900: #020617;
}

body {
  margin: 0;
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

/* Tema claro */
body.theme-light {
  background: var(--green-50);
  color: #022c22;
}

/* Tema oscuro */
body.theme-dark {
  background: linear-gradient(135deg, #06131a 0%, #022c22 45%, #111827 100%);
  color: #f9fafb;
}

/* Top bar */
header.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 28px;
  border-bottom: 1px solid var(--green-200);
  background: #e9fdf2;
}

body.theme-dark header.topbar {
  background: #022c22;
  border-bottom-color: #064e3b;
}

.topbar-left {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 800;
  font-size: 1.25rem;
  color: #065f46;
}

body.theme-dark .topbar-left {
  color: #bbf7d0;
}

.topbar-left span.logo-icon {
  font-size: 1.4rem;
}

.topbar-middle {
  flex: 1;
  display: flex;
  justify-content: center;
}

nav.main-nav {
  display: flex;
  gap: 20px;
  font-size: 0.98rem;
}

nav.main-nav a {
  text-decoration: none;
  color: #065f46;
  font-weight: 600;
}

nav.main-nav a:hover {
  text-decoration: underline;
}

body.theme-dark nav.main-nav a {
  color: #e5e7eb;
}

.topbar-right {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 0.85rem;
}

.theme-toggle a {
  text-decoration: none;
  font-weight: 600;
  color: #047857;
}

body.theme-dark .theme-toggle a {
  color: #bbf7d0;
}

.user-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(22, 163, 74, 0.15);
  font-size: 0.8rem;
}

body.theme-dark .user-pill {
  background: rgba(15,23,42,0.8);
}

.btn {
  padding: 7px 14px;
  border-radius: 999px;
  border: none;
  cursor: pointer;
  font-size: 0.9rem;
}

.btn-primary {
  background: var(--green-600);
  color: #ecfdf5;
}

.btn-primary:hover {
  background: var(--green-700);
}

.btn-danger {
  background: var(--red-600);
  color: #fef2f2;
}

.btn-danger:hover {
  background: #b91c1c;
}

.btn-secondary {
  background: #e5e7eb;
  color: #0f172a;
}

body.theme-dark .btn-secondary {
  background: #334155;
  color: #e5e7eb;
}

.btn-logout {
  background: #16a34a;
  color: white;
}

.btn-logout:hover {
  background: #15803d;
}

/* Contenedor principal */
.container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 24px 20px 40px;
}

/* Cards y dashboard */
.card {
  background: white;
  padding: 18px 18px 20px;
  border-radius: 22px;
  margin-bottom: 18px;
  box-shadow: 0 18px 40px rgba(15, 118, 110, 0.12);
  border: 1px solid #d1fae5;
}

body.theme-dark .card {
  background: rgba(15,23,42,0.96);
  border-color: rgba(22,163,74,0.4);
}

.card h2, .card h3 {
  margin-top: 0;
  color: #065f46;
}

body.theme-dark .card h2,
body.theme-dark .card h3 {
  color: #bbf7d0;
}

.hero-title {
  font-size: 3rem;
  font-weight: 900;
  text-align: center;
  margin: 10px 0 26px;
  background: linear-gradient(90deg, #b91c1c, #4b0082);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.dashboard-grid {
  display: grid;
  grid-template-columns: minmax(0, 2fr) minmax(0, 1.2fr);
  gap: 24px;
  align-items: stretch;
}

@media (max-width: 900px) {
  .dashboard-grid {
    grid-template-columns: 1fr;
  }
}

.kpi-big {
  font-size: 2.6rem;
  font-weight: 800;
  margin: 0 0 10px;
}

.kpi-label {
  font-size: 0.9rem;
  color: #4b5563;
}

body.theme-dark .kpi-label {
  color: #e5e7eb;
}

.table-wrapper {
  margin-top: 12px;
  border-radius: 18px;
  overflow: hidden;
}

/* Tabla */
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}

th, td {
  padding: 9px 10px;
  border-bottom: 1px solid rgba(148,163,184,0.4);
}

th {
  background: #ecfdf3;
  text-align: left;
}

body.theme-dark th {
  background: rgba(30,64,175,0.25);
}

tr:nth-child(even) td {
  background: #f9fffb;
}

body.theme-dark tr:nth-child(even) td {
  background: rgba(15,23,42,0.7);
}

/* Mensajes flash */
.flash-danger { color:#b91c1c; margin-bottom:6px; }
.flash-warning { color:#b45309; margin-bottom:6px; }
.flash-success { color:#166534; margin-bottom:6px; }
.flash-info { color:#0369a1; margin-bottom:6px; }
</style>
"""


# =============================
# LAYOUT GENERAL
# =============================

TPL_LAYOUT = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{{ app_brand }}</title>
  """ + BASE_STYLE + """
</head>
<body class="theme-{{ theme or 'light' }}">

<header class="topbar">
  <div class="topbar-left">
    <span class="logo-icon">💵</span>
    <span>{{ app_brand }}</span>
  </div>

  {% if user %}
  <div class="topbar-middle">
<nav class="main-nav">
    <a href="{{ url_for('index') }}">Inicio</a>
    <a href="{{ url_for('clients') }}">Clientes</a>
    <a href="{{ url_for('loans') }}">Préstamos</a>
    <a href="{{ url_for('route_expenses') }}">Gastos de ruta</a>
    <a href="{{ url_for('audit') }}">Registro</a>
    {% if user.role in ['admin','supervisor'] %}
    <a href="{{ url_for('reassign_clients') }}">Migrar rutas</a>
    <a href="{{ url_for('users') }}">Usuarios</a>
    {% endif %}
</nav>

  </div>

  <div class="topbar-right">
    <span class="theme-toggle">
      Tema:
      {% if theme == 'dark' %}
        <a href="{{ url_for('toggle_theme') }}">🌙 Oscuro</a>
      {% else %}
        <a href="{{ url_for('toggle_theme') }}">☀️ Claro</a>
      {% endif %}
    </span>
    <span class="user-pill">
      <span>👤</span>
      <span>{{ user.username }} ({{ user.role }})</span>
    </span>
    <a class="btn btn-logout" href="{{ url_for('logout') }}">Salir</a>
  </div>
  {% endif %}
</header>

<div class="container">
  {% if flashes %}
    {% for cat, msg in flashes %}
      <div class="flash-{{ cat }}">{{ msg }}</div>
    {% endfor %}
  {% endif %}
  {{ body|safe }}
</div>

</body>
</html>
"""


# =============================
# LOGIN
# =============================

TPL_LOGIN = """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<title>{{ app_brand }} · Login</title>
<meta name="viewport" content="width=device-width, initial-scale=1">

<style>
body {
    background:#e8f5e9;
    font-family: system-ui;
    margin:0;
    padding:0;
}
.header {
    background:#c8e6c9;
    padding:15px;
    text-align:center;
    font-size:22px;
    font-weight:700;
    color:#1b5e20;
}
.card {
    background:white;
    width:90%;
    max-width:400px;
    margin:40px auto;
    padding:25px;
    border-radius:15px;
    box-shadow:0 4px 10px rgba(0,0,0,0.15);
}
h1 {
    font-size:32px;
    text-align:center;
    background: linear-gradient(90deg, #b91c1c, #4b0082);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight:900;
    margin-bottom:20px;
}
label {
    font-size:15px;
    color:#1b5e20;
    font-weight:600;
}
input {
    width:100%;
    padding:10px;
    margin-top:5px;
    margin-bottom:12px;
    border-radius:8px;
    border:1px solid #a5d6a7;
    font-size:16px;
}
button {
    width:100%;
    padding:12px;
    border:none;
    background:#2e7d32;
    color:white;
    font-size:18px;
    font-weight:700;
    border-radius:10px;
    cursor:pointer;
}
.flash-danger {
    background:#ffcdd2;
    padding:10px;
    border-radius:10px;
    color:#b71c1c;
    margin-bottom:15px;
    text-align:center;
    font-weight:600;
}
</style>

</head>
<body>

<div class="header">JDM Cash Now</div>

<div class="card">
    {% if flashes %}
        {% for cat, msg in flashes %}
            <div class="flash-{{ cat }}">{{ msg }}</div>
        {% endfor %}
    {% endif %}

    <h1>JDM Cash Now</h1>

    <form method="post">
        <label>Usuario</label>
        <input name="username" required>

        <label>Contraseña</label>
        <input type="password" name="password" required>

        <button>Entrar</button>
    </form>

    <p style="margin-top:15px;text-align:center;font-size:14px;">
        <a href="{{ url_for('forgot_password') }}" style="color:#2e7d32;text-decoration:none;">
            ¿Olvidó su contraseña?
        </a>
    </p>

    <p style="margin-top:5px;text-align:center;font-size:14px;">
        <a href="https://wa.me/{{ admin_whatsapp }}" target="_blank" style="color:#1b5e20;">
            Recuperar por WhatsApp ({{ admin_whatsapp }})
        </a>
    </p>
</div>

</body>
</html>
"""


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = %s;", (username,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user or not check_password_hash(user["password_hash"], password):
            flash("Usuario o contraseña incorrectos.", "danger")
            return render_template_string(
                TPL_LOGIN,
                flashes=get_flashed_messages(with_categories=True),
                app_brand=APP_BRAND,
                admin_whatsapp=ADMIN_WHATSAPP
            )

        session["user_id"] = user["id"]
        log_action(user["id"], "login", "Inicio de sesión")
        flash(f"Bienvenido, {user['username']}", "success")
        return redirect(url_for("index"))

    return render_template_string(
        TPL_LOGIN,
        flashes=get_flashed_messages(with_categories=True),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP
    )


# ============================================================
#  BOTÓN CAMBIAR TEMA
# ============================================================

@app.route("/toggle-theme")
@login_required
def toggle_theme():
    current = session.get("theme", "light")
    session["theme"] = "dark" if current == "light" else "light"
    ref = request.referrer or url_for("index")
    return redirect(ref)


# ============================================================
#  DASHBOARD (INICIO)
# ============================================================

@app.route("/")
@login_required
def index():
    user = current_user()
    conn = get_conn()
    cur = conn.cursor()

    # Total clientes visibles
    if user["role"] == "cobrador":
        cur.execute("SELECT COUNT(*) AS total FROM clients WHERE created_by = %s;", (user["id"],))
    else:
        cur.execute("SELECT COUNT(*) AS total FROM clients;")
    total_clients = cur.fetchone()["total"]

    # Total préstamos visibles
    if user["role"] == "cobrador":
        cur.execute("SELECT COUNT(*) AS total FROM loans WHERE created_by = %s;", (user["id"],))
    else:
        cur.execute("SELECT COUNT(*) AS total FROM loans;")
    total_loans = cur.fetchone()["total"]

    # Préstamos activos + capital restante (baja cuando pagan capital)
    if user["role"] == "cobrador":
        cur.execute("""
            SELECT COUNT(*) AS c, COALESCE(SUM(remaining),0) AS total_capital
            FROM loans
            WHERE status = 'activo' AND created_by = %s;
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT COUNT(*) AS c, COALESCE(SUM(remaining),0) AS total_capital
            FROM loans
            WHERE status = 'activo';
        """)
    row = cur.fetchone()
    active_loans = row["c"]
    active_capital = row["total_capital"]

    # Últimos préstamos activos con cliente
    if user["role"] == "cobrador":
        cur.execute("""
            SELECT l.id, l.amount, l.rate, l.frequency, l.start_date,
                   c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id = l.client_id
            WHERE l.status = 'activo' AND l.created_by = %s
            ORDER BY l.id DESC
            LIMIT 10;
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT l.id, l.amount, l.rate, l.frequency, l.start_date,
                   c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id = l.client_id
            WHERE l.status = 'activo'
            ORDER BY l.id DESC
            LIMIT 10;
        """)
    last_loans = cur.fetchall()

    cur.close()
    conn.close()

    # Filas de la tabla "Préstamos activos" con interés próximo
    last_rows = ""
    for l in last_loans:
        rate = float(l["rate"] or 0)
        amount = float(l["amount"] or 0)
        next_interest = amount * rate / 100.0  # interés del próximo período
        freq_label = {
            "diario": "Diario",
            "semanal": "Semanal",
            "quincenal": "Quincenal",
            "mensual": "Mensual",
        }.get(l["frequency"], l["frequency"])
        last_rows += f"""
        <tr>
          <td>#{l['id']}</td>
          <td>{l['first_name']} {l['last_name']}</td>
          <td>{fmt_money(amount)}</td>
          <td>{rate:.2f}% / {freq_label}</td>
          <td>{l['start_date']}</td>
          <td>{fmt_money(next_interest)}</td>
        </tr>
        """

    body = f"""
    <div class="hero-title">JDM Cash Now</div>

    <div class="dashboard-grid">
      <section class="card">
        <h2>Préstamos activos</h2>
        <p class="kpi-big">{active_loans}</p>
        <p class="kpi-label">Últimos activos (con nombre del cliente):</p>

        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Cliente</th>
                <th>Capital</th>
                <th>% / Frecuencia</th>
                <th>Inicio</th>
                <th>Interés próximo</th>
              </tr>
            </thead>
            <tbody>
              {last_rows}
            </tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <h2>Capital prestado</h2>
        <p class="kpi-big">{fmt_money(active_capital)}</p>
        <p class="kpi-label">
          Admin puede editar/eliminar cada préstamo desde
          <a href="{url_for('loans')}">Préstamos</a>.
        </p>
        <p>
          <a class="btn btn-primary" href="{url_for('loans')}">Ir a Préstamos</a>
        </p>
      </section>
    </div>

    <div class="dashboard-grid" style="grid-template-columns: repeat(2, minmax(0, 1fr)); margin-top: 18px;">
      <section class="card">
        <h3>Total clientes</h3>
        <p class="kpi-big">{total_clients}</p>
      </section>

      <section class="card">
        <h3>Total préstamos</h3>
        <p class="kpi-big">{total_loans}</p>
      </section>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  USUARIOS
# ============================================================

@app.route("/users")
@login_required
@admin_required
def users():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role, created_at FROM users ORDER BY id DESC;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    users_html = "".join([
        f"""
        <tr>
          <td>{u['id']}</td>
          <td>{u['username']}</td>
          <td>{u['role']}</td>
          <td>{u['created_at']}</td>
          <td>
            <form action="{url_for('delete_user', user_id=u['id'])}" method="post" 
                  onsubmit="return confirm('¿Eliminar usuario permanentemente?');" style="display:inline;">
              <input name="pin" placeholder="PIN" required>
              <button class="btn btn-danger">Eliminar</button>
            </form>
          </td>
        </tr>
        """
        for u in rows
    ])

    body = f"""
    <div class='card'>
      <h2>Usuarios</h2>
      <a href="{url_for('new_user')}" class="btn btn-primary">➕ Nuevo usuario</a>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Usuario</th><th>Rol</th><th>Creado</th><th></th>
            </tr>
          </thead>
          <tbody>{users_html}</tbody>
        </table>
      </div>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=current_user(),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


@app.route("/users/new", methods=["GET", "POST"])
@login_required
@admin_required
def new_user():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        role = request.form.get("role")
        pin = request.form.get("pin")

        if pin != ADMIN_PIN:
            flash("PIN incorrecto.", "danger")
            return redirect(url_for("new_user"))

        if not username or not password:
            flash("Datos incompletos.", "danger")
            return redirect(url_for("new_user"))

        pwd = generate_password_hash(password)

        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (username, password_hash, role, created_at)
                VALUES (%s, %s, %s, %s)
            """, (username, pwd, role, datetime.utcnow()))
            conn.commit()
            flash("Usuario creado correctamente.", "success")
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            flash("Ese usuario ya existe.", "danger")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("users"))

    body = """
    <div class='card'>
      <h2>Crear usuario</h2>

      <form method="post">
        <label>Usuario</label>
        <input name="username" required>

        <label>Contraseña</label>
        <input type="password" name="password" required>

        <label>Rol</label>
        <select name="role">
          <option value="cobrador">Cobrador</option>
          <option value="supervisor">Supervisor</option>
          <option value="admin">Admin</option>
        </select>

        <label>PIN admin</label>
        <input name="pin" required>

        <button class="btn btn-primary" style="margin-top:10px;">Crear usuario</button>
      </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=current_user(),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


@app.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_user(user_id):
    pin = request.form.get("pin")
    if pin != ADMIN_PIN:
        flash("PIN incorrecto.", "danger")
        return redirect(url_for("users"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s AND role != 'admin';", (user_id,))

    if cur.rowcount == 0:
        flash("No se puede borrar un administrador.", "warning")
    else:
        flash("Usuario eliminado.", "success")

    conn.commit()
    cur.close()
    conn.close()

    return redirect(url_for("users"))


# ============================================================
#  REASIGNACIÓN MASIVA DE CLIENTES ENTRE COBRADORES
# ============================================================

@app.route("/reassign", methods=["GET", "POST"])
@login_required
@admin_required
def reassign_clients():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, username FROM users WHERE role = 'cobrador';")
    cobradores = cur.fetchall()

    if request.method == "POST":
        from_id = int(request.form.get("from_id"))
        to_id = int(request.form.get("to_id"))

        if from_id == to_id:
            flash("No puedes reasignar al mismo cobrador.", "warning")
            cur.close()
            conn.close()
            return redirect(url_for("reassign_clients"))

        cur.execute("""
            UPDATE clients
            SET created_by = %s
            WHERE created_by = %s;
        """, (to_id, from_id))

        cur.execute("""
            UPDATE loans
            SET created_by = %s
            WHERE created_by = %s;
        """, (to_id, from_id))

        conn.commit()

        flash("Clientes y préstamos reasignados exitosamente.", "success")
        cur.close()
        conn.close()
        return redirect(url_for("reassign_clients"))

    cur.close()
    conn.close()

    opts = "".join([f"<option value='{c['id']}'>{c['username']}</option>" for c in cobradores])

    body = f"""
    <div class='card'>
      <h2>Reasignar clientes entre cobradores</h2>

      <form method="post">
        <label>Cobrador ORIGEN (quien pierde los clientes)</label>
        <select name="from_id" required>{opts}</select>

        <label>Cobrador DESTINO (quien recibirá los clientes)</label>
        <select name="to_id" required>{opts}</select>

        <button class="btn btn-primary" style="margin-top:10px;">Reasignar</button>
      </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=current_user(),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  REASIGNAR UN SOLO CLIENTE A OTRO COBRADOR
# ============================================================

@app.route("/clients/<int:client_id>/reassign", methods=["POST"])
@login_required
@role_required("admin", "supervisor")
def reassign_single_client(client_id):
    new_user_id = request.form.get("new_user_id", type=int)
    if not new_user_id:
        flash("Seleccione un cobrador destino.", "danger")
        return redirect(url_for("client_detail", client_id=client_id))

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM users WHERE id=%s AND role='cobrador';", (new_user_id,))
    row = cur.fetchone()
    if not row:
        flash("Cobrador destino inválido.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    cur.execute("UPDATE clients SET created_by=%s WHERE id=%s;", (new_user_id, client_id))
    cur.execute("UPDATE loans SET created_by=%s WHERE client_id=%s;", (new_user_id, client_id))
    conn.commit()
    cur.close()
    conn.close()

    flash("Cliente y sus préstamos fueron movidos al nuevo cobrador.", "success")
    return redirect(url_for("client_detail", client_id=client_id))


# ============================================================
#  CLIENTES
# ============================================================

@app.route("/clients")
@login_required
def clients():
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    if user["role"] == "cobrador":
        cur.execute("""
            SELECT * FROM clients
            WHERE created_by = %s
            ORDER BY id DESC
        """, (user["id"],))
    else:
        cur.execute("SELECT * FROM clients ORDER BY id DESC")

    rows = cur.fetchall()
    cur.close()
    conn.close()

    body_rows = "".join([
        f"""
        <tr>
          <td>{c['id']}</td>
          <td>{c['first_name']} {c['last_name']}</td>
          <td>{c['phone']}</td>
          <td>{c['address']}</td>
          <td>{c.get('route') or ''}</td>
          <td>{c['created_at']}</td>
          <td>
            <a class="btn btn-secondary" href="{url_for('client_detail', client_id=c['id'])}">Ver</a>
          </td>
        </tr>
        """ for c in rows
    ])

    body = f"""
    <div class="card">
      <h2>Clientes</h2>
      <a class="btn btn-primary" href="{url_for('new_client')}">➕ Nuevo cliente</a>

      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Nombre</th><th>Teléfono</th>
              <th>Dirección</th><th>Ruta</th><th>Creado</th><th></th>
            </tr>
          </thead>
          <tbody>{body_rows}</tbody>
        </table>
      </div>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        flashes=get_flashed_messages(with_categories=True),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        theme=get_theme()
    )


@app.route("/clients/new", methods=["GET", "POST"])
@login_required
def new_client():
    user = current_user()

    if request.method == "POST":
        first = request.form.get("first_name", "").strip()
        last = request.form.get("last_name", "").strip()
        phone = request.form.get("phone", "").strip()
        address = request.form.get("address", "").strip()
        docid = request.form.get("document_id", "").strip()
        route = request.form.get("route", "").strip()

        if not first:
            flash("El nombre es obligatorio.", "danger")
            return redirect(url_for("new_client"))

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO clients (first_name,last_name,phone,address,document_id,route,created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (first, last, phone, address, docid, route, user["id"]))
        conn.commit()
        cur.close()
        conn.close()

        flash("Cliente agregado.", "success")
        return redirect(url_for("clients"))

    body = """
    <div class="card">
      <h2>Nuevo cliente</h2>

      <form method="post">
        <label>Nombre</label>
        <input required name="first_name">

        <label>Apellido</label>
        <input name="last_name">

        <label>Documento</label>
        <input name="document_id">

        <label>Teléfono</label>
        <input name="phone">

        <label>Dirección</label>
        <input name="address">

        <label>Ruta (zona / sector)</label>
        <input name="route">

        <button class="btn btn-primary" style="margin-top:10px;">Guardar</button>
      </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        flashes=get_flashed_messages(with_categories=True),
        admin_whatsapp=ADMIN_WHATSAPP,
        app_brand=APP_BRAND,
        theme=get_theme()
    )


@app.route("/clients/<int:client_id>")
@login_required
def client_detail(client_id):
    user = current_user()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM clients WHERE id=%s", (client_id,))
    client = cur.fetchone()

    if not client:
        flash("Cliente no encontrado.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("clients"))

    if user["role"] == "cobrador" and client["created_by"] != user["id"]:
        flash("No tienes permiso para este cliente.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("clients"))

    cur.execute("""
        SELECT id, amount, remaining, rate, frequency, start_date, total_interest_paid, status, term_count
        FROM loans
        WHERE client_id=%s
        ORDER BY id DESC
    """, (client_id,))
    loans = cur.fetchall()

    # Para mover un solo cliente a otro cobrador
    reassign_block = ""
    if user["role"] in ("admin", "supervisor"):
        cur.execute("SELECT id, username FROM users WHERE role='cobrador' ORDER BY username;")
        cobradores = cur.fetchall()
        options = "".join([
            f"<option value='{u['id']}'>{u['username']}</option>"
            for u in cobradores
        ])
        reassign_block = f"""
        <form method="post" action="{url_for('reassign_single_client', client_id=client_id)}" style="margin-top:10px;">
          <label>Reasignar a cobrador:</label>
          <select name="new_user_id" required>
            <option value="">--Seleccione--</option>
            {options}
          </select>
          <button class="btn btn-primary" style="margin-left:8px;">Mover cliente</button>
        </form>
        """

    cur.close()
    conn.close()

    loans_html = "".join([
        f"""
        <tr>
          <td>{l['id']}</td>
          <td>{fmt_money(l['amount'])}</td>
          <td>{fmt_money(l['remaining'])}</td>
          <td>{l['rate']}%</td>
          <td>{l['frequency']}</td>
          <td>{l['start_date']}</td>
          <td>{fmt_money(l.get('total_interest_paid', 0))}</td>
          <td>{l.get('status', 'activo')}</td>
          <td>
            <a class="btn btn-secondary" href="{url_for('loan_detail', loan_id=l['id'])}">Ver</a>
          </td>
        </tr>
        """
        for l in loans
    ])

    delete_block = ""
    if user["role"] == "admin":
        delete_block = f"""
        <form method="post" action="{url_for('delete_client', client_id=client_id)}"
              onsubmit="return confirm('¿Eliminar cliente con todos préstamos?');">
          <input name="pin" placeholder="PIN" required>
          <button class="btn btn-danger">Eliminar cliente</button>
        </form>
        """

    body = f"""
    <div class="card">
      <h2>Cliente {client['first_name']} {client['last_name']}</h2>
      <p>Tel: {client['phone']}</p>
      <p>Dirección: {client['address']}</p>
      <p>Documento: {client['document_id']}</p>
      <p>Ruta: {client.get('route') or ''}</p>
      {delete_block}
      {reassign_block}
    </div>

    <div class="card">
      <h3>Préstamos</h3>
      <a class="btn btn-primary" href="/loans/new?client_id={client_id}">➕ Nuevo préstamo</a>

      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Monto</th><th>Restante</th>
              <th>%</th><th>Frecuencia</th><th>Inicio</th>
              <th>Interés pagado</th><th>Estado</th><th></th>
            </tr>
          </thead>
          <tbody>
            {loans_html}
          </tbody>
        </table>
      </div>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        admin_whatsapp=ADMIN_WHATSAPP,
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


@app.route("/clients/<int:client_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_client(client_id):
    pin = request.form.get("pin")
    if pin != ADMIN_PIN:
        flash("PIN incorrecto.", "danger")
        return redirect(url_for("client_detail", client_id=client_id))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM clients WHERE id=%s", (client_id,))
    conn.commit()
    cur.close()
    conn.close()

    flash("Cliente eliminado.", "success")
    return redirect(url_for("clients"))


# ============================================================
#  PRÉSTAMOS LISTA + CREAR
# ============================================================

@app.route("/loans")
@login_required
def loans():
    user = current_user()
    conn = get_conn()
    cur = conn.cursor()

    if user["role"] == "cobrador":
        cur.execute("""
            SELECT l.id, l.amount, l.remaining, l.rate, l.frequency,
                   l.start_date, l.total_interest_paid, l.status, l.term_count,
                   c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id = l.client_id
            WHERE l.created_by = %s
            ORDER BY l.id DESC
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT l.id, l.amount, l.remaining, l.rate, l.frequency,
                   l.start_date, l.total_interest_paid, l.status, l.term_count,
                   c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id = l.client_id
            ORDER BY l.id DESC
        """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    html_rows = "".join([
        f"""
        <tr>
          <td>{l['id']}</td>
          <td>{l['first_name']} {l['last_name']}</td>
          <td>{fmt_money(l['amount'])}</td>
          <td>{fmt_money(l['remaining'])}</td>
          <td>{l['rate']}%</td>
          <td>{l['frequency']}</td>
          <td>{l['start_date']}</td>
          <td>{fmt_money(l.get('total_interest_paid', 0))}</td>
          <td>{l.get('status', 'activo')}</td>
          <td>
            <a class="btn btn-secondary" href="{url_for('loan_detail', loan_id=l['id'])}">Ver</a>
          </td>
        </tr>
        """ for l in rows
    ])

    body = f"""
    <div class="card">
      <h2>Préstamos</h2>
      <a class="btn btn-primary" href="{url_for('new_loan')}">➕ Nuevo préstamo</a>

      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Cliente</th><th>Capital</th><th>Restante</th>
              <th>%</th><th>Frecuencia</th><th>Inicio</th>
              <th>Interés pagado</th><th>Estado</th><th></th>
            </tr>
          </thead>
          <tbody>{html_rows}</tbody>
        </table>
      </div>
    </div>
    """
    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        flashes=get_flashed_messages(with_categories=True),
        admin_whatsapp=ADMIN_WHATSAPP,
        app_brand=APP_BRAND,
        theme=get_theme()
    )


@app.route("/loans/new", methods=["GET", "POST"])
@login_required
def new_loan():
    user = current_user()
    conn = get_conn()
    cur = conn.cursor()

    # Si viene client_id por querystring, lo usamos para preseleccionar
    pre_client_id = request.args.get("client_id", type=int)

    if user["role"] == "cobrador":
        cur.execute("""
            SELECT id, first_name, last_name
            FROM clients
            WHERE created_by = %s
            ORDER BY first_name, last_name
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT id, first_name, last_name
            FROM clients
            ORDER BY first_name, last_name
        """)
    clients = cur.fetchall()

    if request.method == "POST":
        client_id = request.form.get("client_id", type=int)
        amount = request.form.get("amount", type=float)
        rate = request.form.get("rate", type=float)
        freq = request.form.get("frequency")
        start_str = request.form.get("start_date")
        term_count = request.form.get("term_count", type=int)
        end_str = request.form.get("end_date")

        if not client_id or not amount or not freq or not start_str:
            flash("Complete todos los campos obligatorios.", "danger")
            cur.close()
            conn.close()
            return redirect(url_for("new_loan"))

        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        except ValueError:
            start_date = date.today()

        end_date = None
        if end_str:
            try:
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            except ValueError:
                end_date = None

        cur.execute("""
            INSERT INTO loans
            (client_id, amount, rate, frequency, start_date,
             created_by, remaining, total_interest_paid, status,
             term_count, end_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id;
        """, (
            client_id,
            amount,
            rate or 0,
            freq,
            start_date,
            user["id"],
            amount,
            0,
            "activo",
            term_count,
            end_date
        ))
        new_id = cur.fetchone()["id"]
        conn.commit()
        cur.close()
        conn.close()

        flash("Préstamo creado correctamente.", "success")
        return redirect(url_for("loan_detail", loan_id=new_id))

    options = "".join([
        f"<option value='{c['id']}' {'selected' if pre_client_id and pre_client_id == c['id'] else ''}>{c['first_name']} {c['last_name']}</option>"
        for c in clients
    ])

    cur.close()
    conn.close()

    body = f"""
    <div class="card">
      <h2>Nuevo préstamo</h2>

      <form method="post" id="loan-form">
        <label>Cliente</label>
        <select name="client_id" required>
          <option value="">--Seleccione--</option>
          {options}
        </select>

        <label>Monto ({CURRENCY})</label>
        <input type="number" step="0.01" name="amount" required>

        <label>Interés %</label>
        <input type="number" step="0.01" name="rate" value="0">

        <label>Frecuencia</label>
        <select name="frequency" required>
          <option value="diario">Diario</option>
          <option value="semanal">Semanal</option>
          <option value="quincenal">Quincenal</option>
          <option value="mensual">Mensual</option>
        </select>

        <label>Fecha de inicio (desde)</label>
        <input type="date" name="start_date" value="{date.today()}">

        <label>Número de períodos</label>
        <div style="display:flex; gap:8px; max-width:320px; align-items:center;">
          <select name="term_kind" id="term-kind">
            <option value="semanas" selected>Semanas</option>
            <option value="dias">Días</option>
            <option value="quincenas">Quincenas</option>
            <option value="meses">Meses</option>
          </select>
          <input type="number" name="term_count" min="1" value="1" style="flex:1;">
        </div>
        <small id="term-help" style="font-size:0.8rem;color:#4b5563;">
          Ejemplo: 10 semanas, 5 quincenas, 3 meses, etc.
        </small>

        <label>Fecha final (hasta)</label>
        <input type="date" name="end_date">

        <div id="loan-summary" style="margin-top:10px;font-size:0.9rem;"></div>

        <button class="btn btn-primary" style="margin-top:10px;">Guardar</button>
      </form>
    </div>

    <script>
    (function() {{
      function recalc() {{
        var amount = parseFloat(document.querySelector('input[name="amount"]').value) || 0;
        var rate = parseFloat(document.querySelector('input[name="rate"]').value) || 0;
        var n = parseInt(document.querySelector('input[name="term_count"]').value) || 0;
        var perInterest = amount * rate / 100.0;
        var totalInterest = perInterest * n;
        var total = amount + totalInterest;

        var kindSel = document.getElementById('term-kind');
        var kindText = kindSel ? kindSel.options[kindSel.selectedIndex].text : "períodos";
        var pagoLabel = kindText.toLowerCase();
        var cuota = n > 0 ? (total / n) : 0;

        var box = document.getElementById('loan-summary');
        if (!box) return;
        box.innerHTML =
          "Interés por período: {CURRENCY} " + perInterest.toFixed(2) +
          " · Interés total: {CURRENCY} " + totalInterest.toFixed(2) +
          " · Total a pagar (capital + interés): {CURRENCY} " + total.toFixed(2) +
          " · Pago por " + pagoLabel.slice(0, -1) + ": {CURRENCY} " + cuota.toFixed(2) +
          " · Plazo: " + n + " " + kindText;
      }}
      ['amount','rate','term_count'].forEach(function(name) {{
        var el = document.querySelector('[name=\"' + name + '\"]');
        if (el) el.addEventListener('input', recalc);
      }});
      var kindSelect = document.getElementById('term-kind');
      if (kindSelect) kindSelect.addEventListener('change', recalc);
      recalc();
    }})();
    </script>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  DETALLE DE PRÉSTAMO + PAGOS
# ============================================================

@app.route("/loan/<int:loan_id>")
@login_required
def loan_detail(loan_id):
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    # Obtener préstamo
    cur.execute("""
        SELECT l.*, c.first_name, c.last_name, c.phone
        FROM loans l
        JOIN clients c ON c.id = l.client_id
        WHERE l.id=%s
    """, (loan_id,))
    loan = cur.fetchone()

    if not loan:
        flash("Préstamo no encontrado.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("loans"))

    # Cobrador solo puede ver los suyos
    if user["role"] == "cobrador" and loan["created_by"] != user["id"]:
        flash("No tienes permiso para ver este préstamo.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("loans"))

    # Obtener pagos del préstamo
    cur.execute("""
        SELECT *
        FROM payments
        WHERE loan_id=%s
        ORDER BY id DESC
    """, (loan_id,))
    payments = cur.fetchall()

    # ===== Cálculo de cuotas y totales =====
    term_count = loan.get("term_count") or 0
    try:
        term_count_int = int(term_count)
    except (TypeError, ValueError):
        term_count_int = 0

    amount = float(loan["amount"] or 0)
    rate = float(loan["rate"] or 0)

    per_interest = amount * rate / 100.0
    total_interest = per_interest * term_count_int
    total_to_pay = amount + total_interest
    installment = total_to_pay / term_count_int if term_count_int > 0 else 0.0

    # Pagos realizados (cualquier tipo) para calcular restante total
    total_pagado = 0.0
    for p in payments:
        try:
            total_pagado += float(p["amount"] or 0)
        except (TypeError, ValueError):
            pass

    cuotas_pagadas = int(installment > 0 and total_pagado // installment) if installment > 0 else 0
    if cuotas_pagadas > term_count_int:
        cuotas_pagadas = term_count_int

    restante_total = max(0.0, total_to_pay - total_pagado)

    # Palabra para períodos según frecuencia
    freq_label_word = "períodos"
    if loan["frequency"] == "diario":
        freq_label_word = "días"
    elif loan["frequency"] == "semanal":
        freq_label_word = "semanas"
    elif loan["frequency"] == "quincenal":
        freq_label_word = "quincenas"
    elif loan["frequency"] == "mensual":
        freq_label_word = "meses"

    if freq_label_word.endswith("s"):
        pago_label = freq_label_word[:-1]
    else:
        pago_label = "período"

    if term_count_int > 0:
        cuotas_resumen = f"{cuotas_pagadas}/{term_count_int}"
    else:
        cuotas_resumen = "N/A"

    # ===== Cálculo aproximado de atraso según frecuencia / interés =====
    today = date.today()
    start_date = loan["start_date"]
    freq = loan["frequency"]

    step_days = 7
    if freq == "diario":
        step_days = 1
    elif freq == "semanal":
        step_days = 7
    elif freq == "quincenal":
        step_days = 14
    elif freq == "mensual":
        step_days = 30

    overdue_html = ""
    if isinstance(start_date, date) and rate > 0:
        elapsed_days = max(0, (today - start_date).days)
        expected_periods = elapsed_days // step_days
        if term_count_int:
            expected_periods = min(expected_periods, term_count_int)

        per_interest_tmp = amount * rate / 100.0
        paid_interest = float(loan.get("total_interest_paid") or 0)
        paid_periods = int(paid_interest // per_interest_tmp) if per_interest_tmp > 0 else 0
        overdue_periods = max(0, expected_periods - paid_periods)

        interest_due = per_interest_tmp * expected_periods
        interest_pending = max(0.0, interest_due - paid_interest)

        if overdue_periods > 0:
            if freq in ("semanal", "quincenal"):
                unit = "semana"
            elif freq == "diario":
                unit = "día"
            else:
                unit = "mes"
            plural = "s" if overdue_periods != 1 else ""
            overdue_html = f"""
            <p style="color:#dc2626;font-weight:bold;">
              ⚠ Tiene {overdue_periods} {unit}{plural} atrasada(s).
            </p>
            <p style="color:#b91c1c;">
              Interés pendiente aproximado: {fmt_money(interest_pending)}.
            </p>
            """

    # Botón de recibo si está cerrado (admin)
    receipt_btn = ""
    if (loan.get("status") or "").lower() == "cerrado":
        msg = f"Recibo préstamo #{loan_id} pagado. Cliente: {loan['first_name']} {loan['last_name']}. Monto: {fmt_money(loan['amount'])}."
        wa_url = f"https://wa.me/{ADMIN_WHATSAPP}?text={quote_plus(msg)}"
        receipt_btn = f"""
        <p>
          <a class="btn btn-secondary" target="_blank" href="{wa_url}">
            📲 Enviar recibo por WhatsApp al administrador
          </a>
        </p>
        """

    # Enviar factura al cliente por WhatsApp / SMS
    client_phone = (loan.get("phone") or "").strip()
    client_phone_digits = "".join(ch for ch in client_phone if ch.isdigit())
    invoice_buttons = ""
    inv_msg = (
        f"Hola {loan['first_name']}, este es el detalle de su préstamo #{loan_id}: "
        f"Capital {fmt_money(loan['amount'])}, interés total estimado {fmt_money(total_interest)}, "
        f"total a pagar {fmt_money(total_to_pay)}, plazo {term_count_int} {freq_label_word}, "
        f"pago por {pago_label} {fmt_money(installment)}. "
        f"Restante aproximado: {fmt_money(restante_total)}."
    )
    if client_phone_digits:
        wa_client_url = f"https://wa.me/{client_phone_digits}?text={quote_plus(inv_msg)}"
        invoice_buttons = f"""
        <p>
          <a class="btn btn-secondary" target="_blank" href="{wa_client_url}">
            📲 Enviar factura al cliente por WhatsApp
          </a>
        </p>
        <p style="font-size:0.9rem;">
          Texto para SMS / nota:<br>
          <textarea rows="3" style="width:100%;">{inv_msg}</textarea>
        </p>
        """

    # Tabla de pagos
    payments_html = "".join([
        f"""
        <tr>
            <td>{p['id']}</td>
            <td>{p['date']}</td>
            <td>{fmt_money(p['amount'])}</td>
            <td>{p['type']}</td>
            <td>{p['note'] or ''}</td>
        </tr>
        """ for p in payments
    ])

    cur.close()
    conn.close()

    body = f"""
    <div class="card">
        <h2>Préstamo #{loan_id}</h2>
        <p><strong>Cliente:</strong> {loan['first_name']} {loan['last_name']}</p>
        <p><strong>Teléfono cliente:</strong> {loan.get('phone') or ''}</p>

        <p><strong>Monto capital:</strong> {fmt_money(loan['amount'])}</p>
        <p><strong>Interés total estimado:</strong> {fmt_money(total_interest)}</p>
        <p><strong>Total a pagar (capital + interés):</strong> {fmt_money(total_to_pay)}</p>
        <p><strong>Plazo:</strong> {term_count_int} {freq_label_word}</p>
        <p><strong>Pago por {pago_label}:</strong> {fmt_money(installment)}</p>

        <p><strong>Semana / cuota actual:</strong> {cuotas_resumen}</p>
        <p><strong>Total restante estimado:</strong> {fmt_money(restante_total)}</p>

        <p><strong>Interés pagado (solo pagos marcados como interés):</strong> {fmt_money(loan['total_interest_paid'])}</p>
        <p><strong>Estado:</strong> {loan['status']}</p>
        <p><strong>Fecha inicio:</strong> {loan['start_date']}</p>
        {overdue_html}
        <a class="btn btn-primary" href="/loan/{loan_id}/payment/new">➕ Registrar pago</a>
        {receipt_btn}
        {invoice_buttons}
    </div>

    <div class="card">
        <h3>Pagos realizados</h3>
        <div class="table-wrapper">
          <table>
              <thead>
                  <tr>
                      <th>ID</th>
                      <th>Fecha</th>
                      <th>Monto</th>
                      <th>Tipo</th>
                      <th>Nota</th>
                  </tr>
              </thead>
              <tbody>
                  {payments_html}
              </tbody>
          </table>
        </div>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        flashes=get_flashed_messages(with_categories=True),
        admin_whatsapp=ADMIN_WHATSAPP,
        app_brand=APP_BRAND,
        theme=get_theme()
    )


@app.route("/loan/<int:loan_id>/payment/new", methods=["GET", "POST"])
@login_required
def new_payment(loan_id):
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    # Obtener préstamo
    cur.execute("""
        SELECT l.*, c.first_name, c.last_name
        FROM loans l
        JOIN clients c ON c.id = l.client_id
        WHERE l.id=%s
    """, (loan_id,))
    loan = cur.fetchone()

    if not loan:
        flash("Préstamo no encontrado.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("loans"))

    if user["role"] == "cobrador" and loan["created_by"] != user["id"]:
        flash("No tienes permiso para este préstamo.", "danger")
        cur.close()
        conn.close()
        return redirect(url_for("loans"))

    if request.method == "POST":
        amount = request.form.get("amount", type=float)
        pay_type = request.form.get("type")  # "capital" / "interes" / "cuota"
        note = request.form.get("note", "")
        date_str = request.form.get("date")

        if not amount or amount <= 0 or pay_type not in ("capital", "interes", "cuota"):
            flash("Datos de pago inválidos.", "danger")
            cur.close()
            conn.close()
            return redirect(url_for("new_payment", loan_id=loan_id))

        try:
            pay_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            pay_date = date.today()

        # Insertar pago
        cur.execute("""
            INSERT INTO payments (loan_id, amount, type, note, date, created_by)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (loan_id, amount, pay_type, note, pay_date, user["id"]))

        # Valores actuales para remaining / interés
        current_remaining = float(loan["remaining"] or 0)
        total_interest_paid = float(loan["total_interest_paid"] or 0)

        # Actualizar préstamo según tipo de pago
        if pay_type == "capital":
            new_remaining = current_remaining - amount
            if new_remaining < 0:
                new_remaining = 0.0
            cur.execute("""
                UPDATE loans
                SET remaining = %s
                WHERE id = %s
            """, (new_remaining, loan_id))
            current_remaining = new_remaining
        elif pay_type == "interes":
            total_interest_paid += amount
            cur.execute("""
                UPDATE loans
                SET total_interest_paid = %s
                WHERE id = %s
            """, (total_interest_paid, loan_id))
        # pay_type == "cuota": no tocamos remaining ni total_interest_paid directamente

        # ===== Cerrar préstamo automáticamente si se pagó todo el total (capital + interés) =====
        amount_capital = float(loan["amount"] or 0)
        rate = float(loan["rate"] or 0)
        term_count = loan.get("term_count") or 0
        try:
            term_count_int = int(term_count)
        except (TypeError, ValueError):
            term_count_int = 0

        per_interest = amount_capital * rate / 100.0
        total_interest = per_interest * term_count_int
        total_to_pay = amount_capital + total_interest

        # Suma de TODOS los pagos registrados del préstamo
        cur.execute("SELECT COALESCE(SUM(amount),0) AS s FROM payments WHERE loan_id=%s", (loan_id,))
        total_pagado = float(cur.fetchone()["s"] or 0)

        new_status = loan["status"] or "activo"
        if total_to_pay > 0:
            if total_pagado >= total_to_pay - 0.01:  # margen pequeño por redondeo
                new_status = "cerrado"
        else:
            # Si no hay intereses definidos usamos remaining para cerrar
            if current_remaining <= 0:
                new_status = "cerrado"

        cur.execute("""
            UPDATE loans
            SET status = %s
            WHERE id = %s
        """, (new_status, loan_id))

        conn.commit()
        cur.close()
        conn.close()

        flash("Pago registrado correctamente.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    # GET → formulario
    body = f"""
    <div class="card">
      <h2>Registrar pago - Préstamo #{loan_id}</h2>
      <p>Cliente: {loan['first_name']} {loan['last_name']}</p>
      <p>Monto capital: {fmt_money(loan['amount'])}</p>
      <p>Monto restante de capital: {fmt_money(loan['remaining'])}</p>

      <form method="post">
        <label>Monto ({CURRENCY})</label>
        <input type="number" step="0.01" name="amount" required>

        <label>Tipo de pago</label>
        <select name="type" required>
          <option value="cuota">Cuota (capital + interés)</option>
          <option value="capital">Solo capital</option>
          <option value="interes">Solo interés</option>
        </select>

        <label>Fecha</label>
        <input type="date" name="date" value="{date.today()}" required>

        <label>Nota (opcional)</label>
        <input name="note">

        <button class="btn btn-primary" style="margin-top:10px;">Guardar pago</button>
      </form>
    </div>
    """

    cur.close()
    conn.close()

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        admin_whatsapp=ADMIN_WHATSAPP,
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  GASTOS DE RUTA / EFECTIVO ENTREGADO
# ============================================================

@app.route("/route-expenses", methods=["GET", "POST"])
@login_required
def route_expenses():
    user = current_user()
    conn = get_conn()
    cur = conn.cursor()

    if request.method == "POST":
        amount = request.form.get("amount", type=float)
        note = request.form.get("note", "")
        date_str = request.form.get("date")

        if not amount or amount <= 0:
            flash("Monto inválido.", "danger")
        else:
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d").date()
            except (TypeError, ValueError):
                d = date.today()
            cur.execute("""
                INSERT INTO cash_reports (user_id, date, amount, note)
                VALUES (%s,%s,%s,%s)
            """, (user["id"], d, amount, note))
            conn.commit()
            flash("Efectivo entregado registrado.", "success")

    # Listado
    if user["role"] == "admin":
        cur.execute("""
            SELECT c.id, c.date, c.amount, c.note, u.username
            FROM cash_reports c
            LEFT JOIN users u ON u.id = c.user_id
            ORDER BY c.date DESC, c.id DESC
            LIMIT 200;
        """)
    else:
        cur.execute("""
            SELECT c.id, c.date, c.amount, c.note, u.username
            FROM cash_reports c
            LEFT JOIN users u ON u.id = c.user_id
            WHERE c.user_id = %s
            ORDER BY c.date DESC, c.id DESC
            LIMIT 200;
        """, (user["id"],))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    rows_html = "".join([
        f"""
        <tr>
          <td>{r['id']}</td>
          <td>{r['date']}</td>
          <td>{fmt_money(r['amount'])}</td>
          <td>{r.get('username') or ''}</td>
          <td>{r.get('note') or ''}</td>
        </tr>
        """
        for r in rows
    ])

    body = f"""
    <div class="card">
      <h2>Gastos de ruta / Efectivo entregado</h2>
      <p>Registre cuánto dinero en efectivo entrega cada trabajador al finalizar su ruta.</p>

      <form method="post">
        <label>Fecha</label>
        <input type="date" name="date" value="{date.today()}">

        <label>Monto entregado ({CURRENCY})</label>
        <input type="number" step="0.01" name="amount" required>

        <label>Nota (opcional)</label>
        <input name="note">

        <button class="btn btn-primary" style="margin-top:10px;">Guardar</button>
      </form>
    </div>

    <div class="card">
      <h3>Historial de efectivo entregado</h3>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Fecha</th><th>Monto</th><th>Usuario</th><th>Nota</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>
    """
    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=user,
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  AUDITORÍA
# ============================================================

@app.route("/audit")
@login_required
@admin_required
def audit():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.created_at, a.action, a.detail, u.username
        FROM audit_log a
        LEFT JOIN users u ON u.id = a.user_id
        ORDER BY a.id DESC
        LIMIT 200;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    rows_html = "".join([
        f"""
        <tr>
          <td>{r['id']}</td>
          <td>{r['created_at']}</td>
          <td>{r['username'] or ''}</td>
          <td>{r['action']}</td>
          <td>{r['detail'] or ''}</td>
        </tr>
        """
        for r in rows
    ])

    body = f"""
    <div class="card">
      <h2>Auditoría</h2>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Fecha</th><th>Usuario</th>
              <th>Acción</th><th>Detalle</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=current_user(),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  LOGOUT Y RECUPERAR PASSWORD
# ============================================================

@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "success")
    return redirect(url_for("login"))


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        flash("Contacte al admin para recuperar su contraseña.", "info")
        return redirect(url_for("login"))

    body = """
    <div class="card">
      <h2>Recuperar contraseña</h2>
      <p>Por seguridad, la recuperación se realiza por el administrador.</p>
      <p>Escríbele por WhatsApp al número mostrado en la parte superior (SOS).</p>
      <form method="post">
        <button class="btn btn-primary">Entendido</button>
      </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        body=body,
        user=current_user(),
        app_brand=APP_BRAND,
        admin_whatsapp=ADMIN_WHATSAPP,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
#  FINAL – EJECUCIÓN
# ============================================================

# Inicializar BD al importar (sirve para Flask 3 + Render, sin before_first_request)
init_db()

if __name__ == "__main__":
    print("[JDM Cash Now] Iniciando servidor…")
    app.run(host="0.0.0.0", port=5000, debug=True)