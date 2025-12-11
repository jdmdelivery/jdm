# ============================================================
#  JDM CASH NOW PRO – Sistema de Préstamos Multi-Rol (PostgreSQL)
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
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import os
import secrets
from urllib.parse import quote_plus

# ============================================================
# CONFIGURACIÓN PRINCIPAL
# ============================================================

APP_BRAND = "JDM Cash Now Pro"
CURRENCY = "RD$"
ADMIN_PIN = os.getenv("ADMIN_PIN", "5555")
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "3128565688")

ROLES = ("admin", "supervisor", "cobrador")

# ============================================================
# CONEXIÓN A LA BASE DE DATOS
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("❌ ERROR: No está configurada la variable DATABASE_URL.")

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# ============================================================
# FORMATO DE MONEDA
# ============================================================

def fmt_money(v):
    try:
        return f"{CURRENCY}{float(v):,.2f}"
    except:
        return f"{CURRENCY}0.00"

# ============================================================
# FLASK APP
# ============================================================

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# ============================================================
# SESIÓN / USUARIO ACTUAL
# ============================================================

def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id=%s", (uid,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user

# ============================================================
# DECORADORES DE SEGURIDAD
# ============================================================

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user():
            flash("Debe iniciar sesión.", "danger")
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user or user["rol"] != "admin":
            flash("Acceso denegado.", "danger")
            return redirect(url_for("index"))
        return fn(*args, **kwargs)
    return wrapper

def role_required(allowed_roles):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user or user["rol"] not in allowed_roles:
                flash("No tiene permiso.", "danger")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator

# ============================================================
# TEMA CLARO / OSCURO
# ============================================================

def get_theme():
    return session.get("theme", "light")

@app.route("/toggle-theme")
def toggle_theme():
    session["theme"] = "dark" if get_theme() == "light" else "light"
    return redirect(request.referrer or url_for("index"))

# ============================================================
# AUDITORÍA
# ============================================================

def log_action(user_id, action, detail=""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO audit_log (user_id, action, detail)
        VALUES (%s,%s,%s)
    """, (user_id, action, detail))
    conn.commit()
    cur.close()
    conn.close()

# ============================================================
# LAYOUT PREMIUM (GREEN)
# ============================================================

BASE_STYLE = """
<style>
body {
    margin: 0; 
    font-family: system-ui; 
    background-color: {% if theme=='dark' %}#121212{% else %}#f2faf4{% endif %};
    color: {% if theme=='dark' %}#e8e8e8{% else %}#222{% endif %};
}

/* NAVBAR SUPERIOR */
.navbar {
    background: #1b5e20; 
    padding: 12px 18px; 
    display: flex; 
    align-items: center;
    justify-content: space-between;
    color: white;
}
.navbar a {
    color: white; 
    text-decoration: none; 
    margin-right: 15px;
    font-weight: 500;
}
.nav-links {
    display: flex; 
    gap: 14px;
}

/* CONTENIDO */
.container {
    width: 92%;
    max-width: 1200px;
    margin: 25px auto;
}

/* CARDS */
.card {
    background: {% if theme=='dark' %}#1e1e1e{% else %}white{% endif %};
    padding: 22px;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    margin-bottom: 20px;
}

/* BOTONES */
.btn {
    padding: 8px 16px;
    border-radius: 6px;
    text-decoration: none;
    font-weight: 600;
    display: inline-block;
    cursor: pointer;
}
.btn-primary {
    background: #2e7d32; 
    color: white;
}
.btn-secondary {
    background: #004d40; 
    color: white;
}
.btn-danger {
    background: #b71c1c; 
    color: white;
}

/* TABLAS */
table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 12px;
}
table th, table td {
    padding: 10px;
    border-bottom: 1px solid #ccc;
}
table th {
    background: {% if theme=='dark' %}#333{% else %}#e8f5e9{% endif %};
}

/* INPUTS */
input, select {
    width: 100%;
    padding: 10px;
    margin-top: 6px;
    border-radius: 6px;
    border: 1px solid #aaa;
    background: {% if theme=='dark' %}#2c2c2c{% else %}white{% endif %};
    color: inherit;
}

/* FLASHES */
.flash-success {background:#c8e6c9;padding:10px;border-radius:6px;color:#256029;margin-bottom:10px;}
.flash-danger {background:#ffcdd2;padding:10px;border-radius:6px;color:#b71c1c;margin-bottom:10px;}
.flash-warning {background:#fff9c4;padding:10px;border-radius:6px;color:#8a6d00;margin-bottom:10px;}
</style>
"""

TPL_LAYOUT = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>{{ app_brand }}</title>
""" + BASE_STYLE + """
</head>
<body>

<!-- NAVBAR SUPERIOR -->
<div class="navbar">
    <div><b>{{ app_brand }}</b></div>
    <div class="nav-links">
        <a href="/">Inicio</a>
        <a href="/clients">Clientes</a>
        <a href="/loans">Préstamos</a>
        <a href="/route-expenses">Gastos</a>
        {% if user.rol == 'admin' %}
            <a href="/audit">Auditoría</a>
        {% endif %}
        <a href="/toggle-theme">🌓 Tema</a>
        <a href="/logout">Salir</a>
    </div>
</div>

<div class="container">
    {% for cat,msg in flashes %}
        <div class="flash-{{cat}}">{{msg}}</div>
    {% endfor %}

    {{ body }}
</div>

</body>
</html>
"""
# ============================================================
# LOGIN
# ============================================================

TPL_LOGIN = """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<title>{{ app_brand }} · Login</title>
<style>
body { background:#e8f5e9; font-family: system-ui; }
.card {
    background:white; width:90%; max-width:400px;
    margin:40px auto; padding:25px;
    border-radius:15px; box-shadow:0 4px 10px rgba(0,0,0,0.15);
}
.flash-danger { background:#ffcdd2; padding:10px; border-radius:10px; color:#b71c1c; }
button {
    width:100%; padding:10px; margin-top:15px;
    background:#2e7d32; color:white; border:none;
    border-radius:6px; cursor:pointer;
}
input {
    width:100%; padding:10px; margin-top:8px;
    border:1px solid #bbb; border-radius:6px;
}
</style>
</head>
<body>

<div class="card">
    {% for cat, msg in flashes %}
      <div class="flash-{{ cat }}">{{ msg }}</div>
    {% endfor %}

    <h1 style="text-align:center;">{{ app_brand }}</h1>

    <form method="post">
        <label>Usuario</label>
        <input name="username" required>

        <label>Contraseña</label>
        <input type="password" name="password" required>

        <button>Entrar</button>
    </form>
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
        cur.execute("SELECT * FROM users WHERE username=%s;", (username,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user or not check_password_hash(user["password_hash"], password):
            flash("Usuario o contraseña incorrectos.", "danger")
            return render_template_string(
                TPL_LOGIN,
                app_brand=APP_BRAND,
                flashes=get_flashed_messages(with_categories=True)
            )

        session["user_id"] = user["id"]
        log_action(user["id"], "login")
        return redirect(url_for("index"))

    return render_template_string(
        TPL_LOGIN,
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True)
    )

# ============================================================
# DASHBOARD (INICIO)
# ============================================================

@app.route("/")
@login_required
def index():
    user = current_user()
    conn = get_conn()
    cur = conn.cursor()

    # Total clientes visibles
    if user["rol"] == "cobrador":
        cur.execute("SELECT COUNT(*) AS total FROM clients WHERE created_by=%s;",
                    (user["id"],))
    else:
        cur.execute("SELECT COUNT(*) AS total FROM clients;")
    total_clients = cur.fetchone()["total"]

    # Total préstamos
    if user["rol"] == "cobrador":
        cur.execute("""
            SELECT COUNT(*) AS total
            FROM loans
            WHERE created_by=%s
        """, (user["id"],))
    else:
        cur.execute("SELECT COUNT(*) AS total FROM loans;")
    total_loans = cur.fetchone()["total"]

    # Activos + capital
    if user["rol"] == "cobrador":
        cur.execute("""
            SELECT COUNT(*) AS c,
                   COALESCE(SUM(remaining),0) AS capital
            FROM loans
            WHERE status='activo' AND created_by=%s;
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT COUNT(*) AS c,
                   COALESCE(SUM(remaining),0) AS capital
            FROM loans
            WHERE status='activo';
        """)
    row = cur.fetchone()
    active_loans = row["c"]
    active_capital = row["capital"]

    # Últimos préstamos
    if user["rol"] == "cobrador":
        cur.execute("""
            SELECT l.*, c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id=l.client_id
            WHERE l.created_by=%s
            ORDER BY l.id DESC
            LIMIT 10
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT l.*, c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id=l.client_id
            ORDER BY l.id DESC
            LIMIT 10
        """)

    last_loans = cur.fetchall()

    cur.close()
    conn.close()

    # Filas
    last_rows = ""
    for l in last_loans:
        per_int = l["amount"] * l["rate"] / 100
        freq_lbl = {
            "diario": "Diario",
            "semanal": "Semanal",
            "quincenal": "Quincenal",
            "mensual": "Mensual"
        }.get(l["frequency"], l["frequency"])
        last_rows += f"""
        <tr>
            <td>{l['id']}</td>
            <td>{l['first_name']} {l['last_name']}</td>
            <td>{fmt_money(l['amount'])}</td>
            <td>{l['rate']}% / {freq_lbl}</td>
            <td>{l['start_date']}</td>
            <td>{fmt_money(per_int)}</td>
        </tr>
        """

    body = f"""
    <h1>Bienvenido, {user['username']}</h1>

    <div class="card">
        <h2>Resumen general</h2>
        <p><b>Clientes:</b> {total_clients}</p>
        <p><b>Préstamos:</b> {total_loans}</p>
        <p><b>Préstamos activos:</b> {active_loans}</p>
        <p><b>Capital activo:</b> {fmt_money(active_capital)}</p>
    </div>

    <div class="card">
        <h3>Últimos préstamos</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Cliente</th><th>Monto</th>
                    <th>%</th><th>Inicio</th><th>Interés</th>
                </tr>
            </thead>
            <tbody>{last_rows}</tbody>
        </table>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT,
        app_brand=APP_BRAND,
        body=body,
        user=user,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )

# ============================================================
# CLIENTES — LISTA
# ============================================================

@app.route("/clients")
@login_required
def clients():
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    if user["rol"] == "cobrador":
        cur.execute("""
            SELECT *
            FROM clients
            WHERE created_by=%s
            ORDER BY id DESC
        """, (user["id"],))
    else:
        cur.execute("SELECT * FROM clients ORDER BY id DESC")

    rows = cur.fetchall()
    cur.close()
    conn.close()

    html_rows = "".join([
        f"""
        <tr>
            <td>{c['id']}</td>
            <td>{c['first_name']} {c['last_name']}</td>
            <td>{c['phone']}</td>
            <td>{c['address']}</td>
            <td>{c.get('route','')}</td>
            <td>{c['created_at']}</td>
            <td>
                <a class='btn btn-secondary' href='/clients/{c['id']}'>Ver</a>
            </td>
        </tr>
        """ for c in rows
    ])

    body = f"""
    <div class="card">
        <h2>Clientes</h2>
        <a href="/clients/new" class="btn btn-primary">➕ Nuevo cliente</a>

        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Nombre</th><th>Teléfono</th>
                    <th>Dirección</th><th>Ruta</th><th>Creado</th><th></th>
                </tr>
            </thead>
            <tbody>{html_rows}</tbody>
        </table>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=user, app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )

# ============================================================
# NUEVO CLIENTE
# ============================================================

@app.route("/clients/new", methods=["GET", "POST"])
@login_required
def new_client():
    user = current_user()

    if request.method == "POST":
        first = request.form.get("first_name").strip()
        last = request.form.get("last_name").strip()
        phone = request.form.get("phone").strip()
        address = request.form.get("address").strip()
        docid = request.form.get("document_id").strip()
        route = request.form.get("route").strip()

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

        flash("Cliente creado correctamente.", "success")
        return redirect(url_for("clients"))

    body = """
    <div class="card">
        <h2>Nuevo cliente</h2>
        <form method="post">
            <label>Nombre</label><input name="first_name" required>
            <label>Apellido</label><input name="last_name">
            <label>Documento</label><input name="document_id">
            <label>Teléfono</label><input name="phone">
            <label>Dirección</label><input name="address">
            <label>Ruta</label><input name="route">
            <button class="btn btn-primary" style="margin-top:12px;">Guardar</button>
        </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=user, app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )

# ============================================================
# DETALLE DEL CLIENTE
# ============================================================

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
        return redirect(url_for("clients"))

    # Seguridad cobrador
    if user["rol"] == "cobrador" and client["created_by"] != user["id"]:
        flash("No tiene permiso para ver este cliente.", "danger")
        return redirect(url_for("clients"))

    # Préstamos del cliente
    cur.execute("""
        SELECT *
        FROM loans
        WHERE client_id=%s
        ORDER BY id DESC
    """, (client_id,))
    loans = cur.fetchall()

    loans_html = "".join([
        f"""
        <tr>
            <td>{l['id']}</td>
            <td>{fmt_money(l['amount'])}</td>
            <td>{fmt_money(l['remaining'])}</td>
            <td>{l['rate']}%</td>
            <td>{l['frequency']}</td>
            <td>{l['start_date']}</td>
            <td>{l['status']}</td>
            <td><a class='btn btn-secondary' href='/loan/{l['id']}'>Ver</a></td>
        </tr>
        """ for l in loans
    ])

    # Reasignación a cobrador
    reassign_block = ""
    if user["rol"] in ("admin", "supervisor"):
        cur.execute("SELECT id, username FROM users WHERE rol='cobrador'")
        cobradores = cur.fetchall()

        opts = "".join([f"<option value='{u['id']}'>{u['username']}</option>"
                        for u in cobradores])

        reassign_block = f"""
        <form method='post' action='/clients/{client_id}/reassign'>
            <label>Reasignar a cobrador:</label>
            <select name='new_user_id'>{opts}</select>
            <button class='btn btn-primary'>Mover</button>
        </form>
        """

    cur.close()
    conn.close()

    body = f"""
    <div class='card'>
        <h2>Cliente: {client['first_name']} {client['last_name']}</h2>
        <p><b>Teléfono:</b> {client['phone']}</p>
        <p><b>Dirección:</b> {client['address']}</p>
        <p><b>Documento:</b> {client['document_id']}</p>
        <p><b>Ruta:</b> {client['route']}</p>
        {reassign_block}
    </div>

    <div class='card'>
        <h3>Préstamos</h3>
        <a href='/loans/new?client_id={client_id}' class='btn btn-primary'>➕ Nuevo préstamo</a>

        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Monto</th><th>Restante</th>
                    <th>%</th><th>Freq</th><th>Inicio</th><th>Estado</th><th></th>
                </tr>
            </thead>
            <tbody>{loans_html}</tbody>
        </table>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=user, app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )

# ============================================================
# REASIGNAR UN CLIENTE
# ============================================================

@app.route("/clients/<int:client_id>/reassign", methods=["POST"])
@login_required
def reassign_single_client(client_id):
    user = current_user()
    if user["rol"] not in ("admin", "supervisor"):
        flash("No tiene permiso.", "danger")
        return redirect(url_for("clients"))

    new_uid = request.form.get("new_user_id")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("UPDATE clients SET created_by=%s WHERE id=%s",
                (new_uid, client_id))
    cur.execute("UPDATE loans SET created_by=%s WHERE client_id=%s",
                (new_uid, client_id))

    conn.commit()
    cur.close()
    conn.close()

    flash("Cliente reasignado correctamente.", "success")
    return redirect(url_for("client_detail", client_id=client_id))

# ============================================================
# REASIGNACIÓN MASIVA ENTRE COBRADORES
# ============================================================

@app.route("/reassign", methods=["GET", "POST"])
@login_required
@admin_required
def reassign_clients():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, username FROM users WHERE rol='cobrador'")
    cobradores = cur.fetchall()

    if request.method == "POST":
        from_id = int(request.form.get("from_id"))
        to_id = int(request.form.get("to_id"))

        if from_id == to_id:
            flash("No puede reasignar al mismo cobrador.", "warning")
            return redirect(url_for("reassign_clients"))

        cur.execute("UPDATE clients SET created_by=%s WHERE created_by=%s",
                    (to_id, from_id))
        cur.execute("UPDATE loans SET created_by=%s WHERE created_by=%s",
                    (to_id, from_id))
        conn.commit()
        flash("Reasignación completada.", "success")
        return redirect(url_for("reassign_clients"))

    # Render
    options = "".join([
        f"<option value='{c['id']}'>{c['username']}</option>"
        for c in cobradores
    ])

    body = f"""
    <div class='card'>
        <h2>Reasignar clientes entre cobradores</h2>
        <form method='post'>
            <label>Cobrador origen</label>
            <select name='from_id'>{options}</select>

            <label>Cobrador destino</label>
            <select name='to_id'>{options}</select>

            <button class='btn btn-primary' style='margin-top:12px;'>Reasignar</button>
        </form>
    </div>
    """

    cur.close()
    conn.close()

    return render_template_string(
        TPL_LAYOUT, body=body, user=current_user(),
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )
# ============================================================
# LISTA DE PRÉSTAMOS
# ============================================================

@app.route("/loans")
@login_required
def loans():
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    if user["rol"] == "cobrador":
        cur.execute("""
            SELECT l.*, c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id=l.client_id
            WHERE l.created_by=%s
            ORDER BY l.id DESC
        """, (user["id"],))
    else:
        cur.execute("""
            SELECT l.*, c.first_name, c.last_name
            FROM loans l
            JOIN clients c ON c.id=l.client_id
            ORDER BY l.id DESC
        """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    html = "".join([
        f"""
        <tr>
            <td>{l['id']}</td>
            <td>{l['first_name']} {l['last_name']}</td>
            <td>{fmt_money(l['amount'])}</td>
            <td>{fmt_money(l['remaining'])}</td>
            <td>{l['rate']}%</td>
            <td>{l['frequency']}</td>
            <td>{l['start_date']}</td>
            <td>{l['status']}</td>
            <td><a class='btn btn-secondary' href='/loan/{l['id']}'>Ver</a></td>
        </tr>
        """
        for l in rows
    ])

    body = f"""
    <div class='card'>
        <h2>Préstamos</h2>
        <a href='/loans/new' class='btn btn-primary'>➕ Crear préstamo</a>

        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Cliente</th><th>Monto</th>
                    <th>Restante</th><th>%</th><th>Freq</th>
                    <th>Inicio</th><th>Estado</th><th></th>
                </tr>
            </thead>
            <tbody>{html}</tbody>
        </table>
    </div>
    """

    return render_template_string(TPL_LAYOUT, body=body, user=user,
                                  flashes=get_flashed_messages(with_categories=True),
                                  app_brand=APP_BRAND, theme=get_theme())


# ============================================================
# FORMULARIO NUEVO PRÉSTAMO (GET)
# ============================================================

@app.route("/loans/new", methods=["GET"])
@login_required
def new_loan_form():
    client_id = request.args.get("client_id", "")

    body = f"""
    <div class='card'>
        <h2>Nuevo Préstamo</h2>

        <form method='post' action='/loans/new'>
            <label>ID Cliente</label>
            <input name='client_id' value='{client_id}' required>

            <label>Monto prestado</label>
            <input type='number' name='amount' step='0.01' required>

            <label>Interés (%)</label>
            <input type='number' name='rate' step='0.01' required>

            <label>Frecuencia</label>
            <select name='frequency' required>
                <option value='diario'>Diario</option>
                <option value='semanal'>Semanal</option>
                <option value='quincenal'>Quincenal</option>
                <option value='mensual'>Mensual</option>
            </select>

            <label>Fecha inicio</label>
            <input type='date' name='start_date' value='{date.today()}' required>

            <label>Cantidad de períodos</label>
            <input type='number' name='term_count' required>

            <label>Tipo de período</label>
            <select name='term_kind'>
                <option value='dias'>Días</option>
                <option value='semanas'>Semanas</option>
            </select>

            <label>Fee (%)</label>
            <input type='number' name='fee_percent' value='10' step='0.01' required>

            <button class='btn btn-primary' style='margin-top:12px;'>Crear préstamo</button>
        </form>
    </div>
    """

    return render_template_string(TPL_LAYOUT, body=body,
                                  user=current_user(), app_brand=APP_BRAND,
                                  flashes=get_flashed_messages(with_categories=True),
                                  theme=get_theme())


# ============================================================
# NUEVO PRÉSTAMO (POST)
# ============================================================

@app.route("/loans/new", methods=["POST"])
@login_required
def new_loan():
    user = current_user()

    client_id = request.form.get("client_id")
    amount = float(request.form.get("amount"))
    rate = float(request.form.get("rate"))
    freq = request.form.get("frequency")
    start_date = request.form.get("start_date")
    term_count = int(request.form.get("term_count"))
    term_kind = request.form.get("term_kind")
    fee_percent = float(request.form.get("fee_percent"))

    # Fee solo editable por el admin
    if user["rol"] != "admin" and fee_percent <= 0:
        flash("Solo el administrador puede poner fee 0%.", "danger")
        return redirect(url_for("new_loan_form"))

    # Fee
    fee_amount = amount * fee_percent / 100
    disbursement = amount - fee_amount

    # Fecha final automática
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    if term_kind == "dias":
        auto_end_date = s + timedelta(days=term_count)
    else:
        auto_end_date = s + timedelta(weeks=term_count)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO loans (
            client_id, amount, rate, frequency, start_date,
            created_by, remaining, total_interest_paid, status,
            term_count, end_date, fee_percent, fee_amount,
            disbursement, auto_end_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'activo',%s,%s,%s,%s,%s,%s)
        RETURNING id;
    """, (
        client_id, amount, rate, freq, start_date,
        user["id"], amount, 0,
        term_count, auto_end_date, fee_percent,
        fee_amount, disbursement, auto_end_date
    ))

    loan_id = cur.fetchone()["id"]

    conn.commit()
    cur.close()
    conn.close()

    flash("Préstamo creado exitosamente.", "success")
    return redirect(url_for("loan_detail", loan_id=loan_id))


# ============================================================
# DETALLE DEL PRÉSTAMO
# ============================================================

@app.route("/loan/<int:loan_id>")
@login_required
def loan_detail(loan_id):
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT l.*, c.first_name, c.last_name, c.phone
        FROM loans l
        JOIN clients c ON c.id=l.client_id
        WHERE l.id=%s
    """, (loan_id,))
    loan = cur.fetchone()

    if not loan:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))

    # Seguridad cobrador
    if user["rol"] == "cobrador" and loan["created_by"] != user["id"]:
        flash("No tiene permiso.", "danger")
        return redirect(url_for("loans"))

    # Cálculos
    per_interest = loan["amount"] * loan["rate"] / 100
    total_interest = per_interest * loan["term_count"]
    total_to_pay = loan["amount"] + total_interest
    installment = total_to_pay / loan["term_count"]

    # Pagos
    cur.execute("SELECT * FROM payments WHERE loan_id=%s ORDER BY id DESC", (loan_id,))
    payments = cur.fetchall()

    total_paid = sum(float(p["amount"]) for p in payments)
    remaining_total = max(total_to_pay - total_paid, 0)

    phone = (loan["phone"] or "").replace(" ", "")
    wa_url = ""
    if phone:
        msg = (
            f"Factura préstamo #{loan_id}%0A"
            f"Monto: {fmt_money(loan['amount'])}%0A"
            f"Interés: {loan['rate']}%%0A"
            f"Total a pagar: {fmt_money(total_to_pay)}%0A"
            f"Cuota: {fmt_money(installment)}%0A"
            f"Fecha final: {loan['auto_end_date']}"
        )
        wa_url = f"https://wa.me/{phone}?text={msg}"

    cur.close()
    conn.close()

    pay_rows = "".join([
        f"""
        <tr>
            <td>{p['id']}</td>
            <td>{p['date']}</td>
            <td>{fmt_money(p['amount'])}</td>
            <td>{p['type']}</td>
        </tr>
        """ for p in payments
    ])

    body = f"""
    <div class='card'>
        <h2>Préstamo #{loan_id}</h2>

        <p><b>Cliente:</b> {loan['first_name']} {loan['last_name']}</p>
        <p><b>Monto prestado:</b> {fmt_money(loan['amount'])}</p>
        <p><b>Total a pagar:</b> {fmt_money(total_to_pay)}</p>
        <p><b>Interés:</b> {fmt_money(per_interest)} por período</p>
        <p><b>Cuota:</b> {fmt_money(installment)}</p>
        <p><b>Restante total:</b> {fmt_money(remaining_total)}</p>
        <p><b>Fecha final:</b> {loan['auto_end_date']}</p>

        <h3>Enviar factura</h3>
        { f"<a class='btn btn-primary' target='_blank' href='{wa_url}'>📲 WhatsApp</a>" if wa_url else "<p>Cliente sin teléfono válido.</p>" }

        <hr>
        <h3>Pagos</h3>

        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Monto</th><th>Tipo</th>
                </tr>
            </thead>
            <tbody>{pay_rows}</tbody>
        </table>

        <a href='/loan/{loan_id}/payment/new' class='btn btn-primary'>➕ Registrar pago</a>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=user,
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
# REGISTRAR PAGO
# ============================================================

@app.route("/loan/<int:loan_id>/payment/new", methods=["GET", "POST"])
@login_required
def new_payment(loan_id):
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM loans WHERE id=%s", (loan_id,))
    loan = cur.fetchone()

    if not loan:
        flash("Préstamo no encontrado.", "danger")
        return redirect(url_for("loans"))

    if request.method == "POST":
        amount = float(request.form.get("amount"))
        type_ = request.form.get("type")
        note = request.form.get("note")
        date_str = request.form.get("date")

        pay_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        cur.execute("""
            INSERT INTO payments (loan_id, amount, type, note, date, created_by)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (loan_id, amount, type_, note, pay_date, user["id"]))

        # Cierre
        per_int = loan["amount"] * loan["rate"] / 100
        total_interest = per_int * loan["term_count"]
        total_to_pay = loan["amount"] + total_interest

        cur.execute("SELECT COALESCE(SUM(amount),0) AS s FROM payments WHERE loan_id=%s",
                    (loan_id,))
        total_paid = float(cur.fetchone()["s"])

        if total_paid >= total_to_pay - 0.01:
            cur.execute("UPDATE loans SET status='cerrado' WHERE id=%s", (loan_id,))

        conn.commit()
        cur.close()
        conn.close()

        flash("Pago registrado.", "success")
        return redirect(url_for("loan_detail", loan_id=loan_id))

    body = f"""
    <div class='card'>
        <h2>Registrar pago — Préstamo #{loan_id}</h2>

        <form method='post'>
            <label>Monto</label>
            <input type='number' name='amount' step='0.01' required>

            <label>Tipo</label>
            <select name='type'>
                <option value='cuota'>Cuota</option>
                <option value='capital'>Capital</option>
                <option value='interes'>Interés</option>
            </select>

            <label>Fecha</label>
            <input type='date' name='date' value='{date.today()}' required>

            <label>Nota</label>
            <input name='note'>

            <button class='btn btn-primary' style='margin-top:12px;'>Guardar</button>
        </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=user,
        app_brand=APP_BRAND,
        flashes=get_flashed_messages(with_categories=True),
        theme=get_theme()
    )


# ============================================================
# GASTOS DE RUTA
# ============================================================

@app.route("/route-expenses", methods=["GET", "POST"])
@login_required
def route_expenses():
    user = current_user()

    conn = get_conn()
    cur = conn.cursor()

    if request.method == "POST":
        amount = float(request.form.get("amount"))
        note = request.form.get("note")
        date_str = request.form.get("date")

        d = datetime.strptime(date_str, "%Y-%m-%d").date()

        cur.execute("""
            INSERT INTO cash_reports (user_id, date, amount, note)
            VALUES (%s,%s,%s,%s)
        """, (user["id"], d, amount, note))

        conn.commit()
        flash("Registro guardado.", "success")

    # Listado
    if user["rol"] == "admin":
        cur.execute("""
            SELECT c.id, c.date, c.amount, c.note, u.username
            FROM cash_reports c
            LEFT JOIN users u ON u.id=c.user_id
            ORDER BY c.date DESC, c.id DESC
            LIMIT 200
        """)
    else:
        cur.execute("""
            SELECT c.id, c.date, c.amount, c.note, u.username
            FROM cash_reports c
            LEFT JOIN users u ON u.id=c.user_id
            WHERE c.user_id=%s
            ORDER BY c.date DESC, c.id DESC
            LIMIT 200
        """, (user["id"],))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    list_html = "".join([
        f"""
        <tr>
            <td>{r['id']}</td>
            <td>{r['date']}</td>
            <td>{fmt_money(r['amount'])}</td>
            <td>{r.get('username','')}</td>
            <td>{r['note'] or ''}</td>
        </tr>
        """ for r in rows
    ])

    body = f"""
    <div class='card'>
        <h2>Gastos de ruta</h2>

        <form method='post'>
            <label>Fecha</label>
            <input type='date' name='date' value='{date.today()}' required>

            <label>Monto</label>
            <input type='number' step='0.01' name='amount' required>

            <label>Nota</label>
            <input name='note'>

            <button class='btn btn-primary' style='margin-top:12px;'>Guardar</button>
        </form>
    </div>

    <div class='card'>
        <h3>Historial</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Monto</th>
                    <th>Usuario</th><th>Nota</th>
                </tr>
            </thead>
            <tbody>{list_html}</tbody>
        </table>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=user,
        flashes=get_flashed_messages(with_categories=True),
        app_brand=APP_BRAND, theme=get_theme()
    )


# ============================================================
# AUDITORÍA
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
        LEFT JOIN users u ON u.id=a.user_id
        ORDER BY a.id DESC
        LIMIT 200
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    html = "".join([
        f"""
        <tr>
            <td>{r['id']}</td>
            <td>{r['created_at']}</td>
            <td>{r.get('username','')}</td>
            <td>{r['action']}</td>
            <td>{r['detail']}</td>
        </tr>
        """ for r in rows
    ])

    body = f"""
    <div class='card'>
        <h2>Auditoría</h2>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Usuario</th>
                    <th>Acción</th><th>Detalle</th>
                </tr>
            </thead>
            <tbody>{html}</tbody>
        </table>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=current_user(),
        flashes=get_flashed_messages(with_categories=True),
        app_brand=APP_BRAND, theme=get_theme()
    )

# ============================================================
# LOGOUT
# ============================================================

@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "success")
    return redirect(url_for("login"))

# ============================================================
# RECUPERAR CONTRASEÑA
# ============================================================

@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        flash("Contacte al administrador para recuperar su contraseña.", "info")
        return redirect(url_for("login"))

    body = """
    <div class='card'>
        <h2>Recuperar contraseña</h2>
        <p>Por seguridad, solo el administrador puede cambiar contraseñas.</p>
        <form method='post'>
            <button class='btn btn-primary'>Entendido</button>
        </form>
    </div>
    """

    return render_template_string(
        TPL_LAYOUT, body=body, user=current_user(),
        flashes=get_flashed_messages(with_categories=True),
        app_brand=APP_BRAND, theme=get_theme()
    )


# ============================================================
# BASE DE DATOS — INIT_DB
# ============================================================

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    try:
        # USERS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                rol VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # CLIENTS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100),
                phone VARCHAR(50),
                address VARCHAR(200),
                document_id VARCHAR(100),
                route VARCHAR(100),
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # LOANS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS loans (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                amount NUMERIC(12,2) NOT NULL,
                rate NUMERIC(5,2) NOT NULL,
                frequency VARCHAR(20) NOT NULL,
                start_date DATE NOT NULL,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                remaining NUMERIC(12,2),
                total_interest_paid NUMERIC(12,2) DEFAULT 0,
                status VARCHAR(20) DEFAULT 'activo',
                term_count INTEGER,
                end_date DATE,
                fee_percent NUMERIC(5,2) DEFAULT 10,
                fee_amount NUMERIC(12,2) DEFAULT 0,
                disbursement NUMERIC(12,2) DEFAULT 0,
                auto_end_date DATE
            );
        """)

        # PAYMENTS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                loan_id INTEGER REFERENCES loans(id) ON DELETE CASCADE,
                amount NUMERIC(12,2) NOT NULL,
                type VARCHAR(20) NOT NULL,
                note TEXT,
                date DATE,
                created_by INTEGER REFERENCES users(id),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # GASTOS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cash_reports (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                date DATE NOT NULL,
                amount NUMERIC(12,2) NOT NULL,
                note TEXT
            );
        """)

        # AUDITORÍA
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                action TEXT,
                detail TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ Error init_db:", e)

    finally:
        cur.close()
        conn.close()


# ============================================================
# EJECUCIÓN FINAL
# ============================================================

init_db()

if __name__ == "__main__":
    print("[JDM Cash Now Pro] Servidor iniciado.")
    app.run(host="0.0.0.0", port=5000, debug=True)

