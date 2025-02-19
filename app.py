import os
import json
import smtplib
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)

# ---------------------------------------------------------
# CONFIGURACIÓN DE CORS
# ---------------------------------------------------------
CORS(app, resources={r"/*": {"origins": [
    "http://kossodo.estilovisual.com",
    "https://kossodo.estilovisual.com",
    "https://atusaludlicoreria.com"
]}})

# ---------------------------------------------------------
# CONFIGURACIÓN DE LA BASE DE DATOS
# ---------------------------------------------------------
DB_CONFIG = {
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'host': os.environ.get('MYSQL_HOST'),
    'database': os.environ.get('MYSQL_DATABASE'),
    'port': 3306
}

# ---------------------------------------------------------
# CREDENCIALES DE EMAIL (DESDE VARIABLES DE ENTORNO)
# ---------------------------------------------------------
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')

# ---------------------------------------------------------
# FUNCIÓN DE CONEXIÓN A BD
# ---------------------------------------------------------
def get_db_connection():
    """
    Retorna la conexión a la base de datos usando DB_CONFIG.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error de conexión: {e}")
        return None

# ---------------------------------------------------------
# CREACIÓN DE TABLAS AL INICIAR LA APLICACIÓN
# ---------------------------------------------------------
# Tablas de inventario (merch)
merch_columns = [
    "id INT AUTO_INCREMENT PRIMARY KEY",
    "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP",
    "responsable VARCHAR(255)",
    "merch_lapiceros_normales INT DEFAULT 0",
    "merch_lapicero_ejecutivos INT DEFAULT 0",
    "merch_blocks INT DEFAULT 0",
    "merch_tacos INT DEFAULT 0",
    "merch_gel_botella INT DEFAULT 0",
    "merch_bolas_antiestres INT DEFAULT 0",
    "merch_padmouse INT DEFAULT 0",
    "merch_bolsa INT DEFAULT 0",
    "merch_lapiceros_esco INT DEFAULT 0",
    "observaciones TEXT"
]
columns_merch_str = ", ".join(merch_columns)

# Tablas de inventario para Kossodo y Kossomet
TABLES_MERCH = [
    "inventario_merch_kossodo",
    "inventario_merch_kossomet"
]

table_queries = {}
for table in TABLES_MERCH:
    table_queries[table] = f"CREATE TABLE IF NOT EXISTS {table} ({columns_merch_str});"

# Tabla de solicitudes
solicitudes_columns = [
    "id INT AUTO_INCREMENT PRIMARY KEY",
    "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP",
    "solicitante VARCHAR(255)",
    "grupo VARCHAR(50)",
    "ruc VARCHAR(50)",
    "fecha_visita DATE",
    "cantidad_packs INT DEFAULT 0",
    "productos TEXT",
    "catalogos TEXT",
    "status VARCHAR(50) DEFAULT 'pending'"
]
table_queries["inventario_solicitudes"] = (
    f"CREATE TABLE IF NOT EXISTS inventario_solicitudes ({', '.join(solicitudes_columns)});"
)

# Tabla de confirmaciones de solicitudes
conf_columns = [
    "id INT AUTO_INCREMENT PRIMARY KEY",
    "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP",
    "solicitud_id INT NOT NULL",
    "confirmador VARCHAR(255) NOT NULL",
    "observaciones TEXT",
    "productos TEXT",  # Aquí se guardará la información en formato JSON
    "grupo VARCHAR(50)",
    "FOREIGN KEY (solicitud_id) REFERENCES inventario_solicitudes(id)"
]
table_queries["inventario_solicitudes_conf"] = (
    f"CREATE TABLE IF NOT EXISTS inventario_solicitudes_conf ({', '.join(conf_columns)});"
)

def create_tables():
    """
    Crea (o verifica) las tablas necesarias al iniciar la aplicación.
    """
    conn = get_db_connection()
    if conn is None:
        print("No se pudo conectar a la base de datos")
        return
    cursor = conn.cursor()
    for table, query in table_queries.items():
        try:
            cursor.execute(query)
            conn.commit()
            print(f"Tabla '{table}' verificada/creada correctamente.")
        except Error as e:
            print(f"Error al crear la tabla '{table}': {e}")
    cursor.close()
    conn.close()

# Ejecutar la creación de tablas al iniciar la aplicación
create_tables()

# ---------------------------------------------------------
# FUNCIÓN PARA ENVIAR CORREO
# ---------------------------------------------------------
def send_email_solicitud(data):
    """
    Envía un correo con la información de la nueva solicitud creada.
    `data` es un diccionario con los campos de la solicitud.
    """
    # Destinatarios a los que quieres enviar la notificación
    recipients = [
        "jcamacho@kossodo.com",
        "rbazan@kossodo.com",
        "eventos@kossodo.com"
    ]

    # Asunto del correo
    subject = f"Nueva Solicitud de Inventario (ID: {data.get('id', 'N/A')})"

    # Cuerpo del correo (texto plano)
    body = f"""
Estimados,

Se ha registrado una nueva solicitud de inventario con la siguiente información:

- ID: {data.get('id', 'N/A')}
- Fecha/Hora de Registro: {data.get('timestamp', 'N/A')}
- Solicitante: {data.get('solicitante', 'N/A')}
- Grupo: {data.get('grupo', 'N/A')}
- RUC: {data.get('ruc', 'N/A')}
- Fecha de Visita: {data.get('fecha_visita', 'N/A')}
- Cantidad de Packs: {data.get('cantidad_packs', 'N/A')}
- Productos: {data.get('productos', '[]')}
- Catálogos: {data.get('catalogos', 'N/A')}
- Estado: {data.get('status', 'pending')}

Por favor, revise la solicitud en el sistema para más detalles.

Saludos cordiales,
Sistema de Inventario
"""

    # Construimos el mensaje
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Conexión y envío por SMTP
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_USER, recipients, msg.as_string())
        server.quit()
        print("Correo enviado exitosamente.")
    except Exception as e:
        print("Error al enviar el correo:", e)


# ---------------------------------------------------------
# ENDPOINTS DE INVENTARIO (MERCH)
# ---------------------------------------------------------

# GET: Obtener registros del inventario
@app.route('/api/inventario', methods=['GET'])
def obtener_inventario():
    tabla_param = request.args.get('tabla')
    if tabla_param not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Parámetro 'tabla' inválido. Use 'kossodo' o 'kossomet'."}), 400

    table_name = f"inventario_merch_{tabla_param}"
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY timestamp DESC;")
        registros = cursor.fetchall()
        return jsonify(registros), 200
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# POST: Agregar un nuevo registro al inventario
@app.route('/api/inventario', methods=['POST'])
def agregar_inventario():
    tabla_param = request.args.get('tabla')
    if tabla_param not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Parámetro 'tabla' inválido. Use 'kossodo' o 'kossomet'."}), 400

    table_name = f"inventario_merch_{tabla_param}"
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    # Recorremos todas las claves que llegan en el JSON
    columnas = []
    valores = []
    for key, val in data.items():
        # Aceptamos las que sean 'responsable', 'observaciones' o empiecen con merch_
        if key in ['responsable', 'observaciones'] or key.startswith('merch_'):
            columnas.append(key)
            valores.append(val)

    if not columnas:
        return jsonify({"error": "No se han enviado campos válidos para insertar."}), 400

    placeholders = ", ".join(["%s"] * len(valores))
    columnas_str = ", ".join(f"`{col}`" for col in columnas)  # Importante usar backticks
    query = f"INSERT INTO {table_name} ({columnas_str}) VALUES ({placeholders});"

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor()
    try:
        cursor.execute(query, tuple(valores))
        conn.commit()
        nuevo_id = cursor.lastrowid
        return jsonify({"message": "Registro agregado exitosamente", "id": nuevo_id}), 201
    except Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# POST: Agregar un nuevo tipo de producto (nueva columna) en la tabla + insertar un registro
@app.route('/api/nuevo_producto', methods=['POST'])
def nuevo_producto():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    grupo = data.get('grupo')
    nombre_producto = data.get('nombre_producto')  # Nombre original (solo informativo)
    columna = data.get('columna')  # Ej: "merch_lapicero_esco"
    cantidad = data.get('cantidad', 0)

    if grupo not in ['kossodo', 'kossomet']:
        return jsonify({"error": "El grupo debe ser 'kossodo' o 'kossomet'."}), 400
    if not columna or not nombre_producto:
        return jsonify({"error": "Faltan datos: nombre_producto o columna."}), 400

    table_name = f"inventario_merch_{grupo}"
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor()
    try:
        # Verificar si la columna ya existe
        db_name = DB_CONFIG['database']
        query_check = """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s;
        """
        cursor.execute(query_check, (db_name, table_name, columna))
        (existe,) = cursor.fetchone()

        if existe == 0:
            # Crear la columna si no existe
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{columna}` INT DEFAULT 0;"
            cursor.execute(alter_sql)
            conn.commit()

        # Insertar un registro con la cantidad inicial
        insert_sql = f"INSERT INTO {table_name} (`{columna}`) VALUES (%s);"
        cursor.execute(insert_sql, (cantidad,))
        conn.commit()

        nuevo_id = cursor.lastrowid
        return jsonify({"message": "Nuevo producto agregado correctamente", "id": nuevo_id}), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# GET: Obtener el stock calculado y actualizar inventario_stock_{grupo}
@app.route('/api/stock', methods=['GET'])
def obtener_stock():
    grupo = request.args.get('grupo')
    if grupo not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Grupo inválido. Use 'kossodo' o 'kossomet'."}), 400

    inventario_table = f"inventario_merch_{grupo}"
    stock_table = f"inventario_stock_{grupo}"
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor(dictionary=True)
    try:
        db_name = DB_CONFIG['database']
        # 1. Obtener columnas "merch_*" en la tabla de inventario
        query_cols = """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME LIKE 'merch\\_%'
        """
        cursor.execute(query_cols, (db_name, inventario_table))
        cols = [row['COLUMN_NAME'] for row in cursor.fetchall()]

        # 2. Sumar totales de inventario por producto
        inventory_totals = {}
        for col in cols:
            query_sum = f"SELECT SUM(`{col}`) AS total FROM {inventario_table}"
            cursor.execute(query_sum)
            result = cursor.fetchone()
            total = result['total'] if result['total'] is not None else 0
            inventory_totals[col] = total

        # 3. Calcular total confirmado por producto (sumando confirmaciones del grupo)
        request_totals = {col: 0 for col in cols}
        query_conf = "SELECT productos FROM inventario_solicitudes_conf WHERE grupo = %s"
        cursor.execute(query_conf, (grupo,))
        conf_rows = cursor.fetchall()
        for row in conf_rows:
            try:
                productos_dict = json.loads(row['productos']) if row['productos'] else {}
            except Exception:
                productos_dict = {}
            for prod, qty in productos_dict.items():
                if prod in request_totals:
                    request_totals[prod] += qty

        # 4. Calcular el stock final por producto: inventario - confirmaciones
        stock = {}
        for col in cols:
            stock[col] = inventory_totals.get(col, 0) - request_totals.get(col, 0)

        # 5. Crear la tabla de stock si no existe
        create_stock_query = f"""
            CREATE TABLE IF NOT EXISTS {stock_table} (
                id INT PRIMARY KEY,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """
        cursor.execute(create_stock_query)
        conn.commit()

        # Verificar qué columnas existen en la tabla de stock
        query_cols_stock = """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME LIKE 'merch\\_%'
        """
        cursor.execute(query_cols_stock, (db_name, stock_table))
        stock_cols_existing = {row['COLUMN_NAME'] for row in cursor.fetchall()}

        # Agregar columnas que falten en la tabla de stock
        for col in cols:
            if col not in stock_cols_existing:
                alter_query = f"ALTER TABLE {stock_table} ADD COLUMN `{col}` INT DEFAULT 0;"
                cursor.execute(alter_query)
                conn.commit()

        # 6. Actualizar (o insertar) la fila con id=1 en la tabla de stock
        columns_list = ', '.join([f"`{col}`" for col in cols])
        placeholders = ', '.join(['%s'] * len(cols))
        values = [stock[col] for col in cols]
        update_parts = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in cols])
        insert_query = f"""
            INSERT INTO {stock_table} (id, {columns_list})
            VALUES (1, {placeholders})
            ON DUPLICATE KEY UPDATE {update_parts};
        """
        cursor.execute(insert_query, values)
        conn.commit()

        # 7. Retornar la fila actualizada de la tabla de stock
        cursor.execute(f"SELECT * FROM {stock_table} WHERE id = 1;")
        stock_row = cursor.fetchone()
        return jsonify(stock_row), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# ENDPOINTS PARA SOLICITUDES
# ---------------------------------------------------------

# POST: Crear una nueva solicitud (ENVÍA CORREO AL FINAL)
@app.route('/api/solicitud', methods=['POST'])
def crear_solicitud():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    solicitante = data.get('solicitante')
    grupo = data.get('grupo')
    ruc = data.get('ruc')
    fecha_visita = data.get('fecha_visita')
    cantidad_packs = data.get('cantidad_packs', 0)
    productos = data.get('productos', [])
    catalogos = data.get('catalogos', "")

    if not solicitante or not grupo or not ruc or not fecha_visita:
        return jsonify({"error": "Faltan campos requeridos: solicitante, grupo, ruc, fecha_visita."}), 400

    productos_str = json.dumps(productos)

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor()
    try:
        # Aseguramos que la tabla existe
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS inventario_solicitudes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                solicitante VARCHAR(255),
                grupo VARCHAR(50),
                ruc VARCHAR(50),
                fecha_visita DATE,
                cantidad_packs INT DEFAULT 0,
                productos TEXT,
                catalogos TEXT,
                status VARCHAR(50) DEFAULT 'pending'
            );
        """
        cursor.execute(create_table_sql)
        conn.commit()

        # Insertar la nueva solicitud
        insert_sql = """
            INSERT INTO inventario_solicitudes
            (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos, catalogos)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        values = (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos_str, catalogos)
        cursor.execute(insert_sql, values)
        conn.commit()

        nuevo_id = cursor.lastrowid

        # Preparar datos para el correo
        # Podríamos recuperar el timestamp exacto de la BD, pero para simplificar usamos datetime.now()
        solicitud_data = {
            "id": nuevo_id,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "solicitante": solicitante,
            "grupo": grupo,
            "ruc": ruc,
            "fecha_visita": fecha_visita,
            "cantidad_packs": cantidad_packs,
            "productos": productos,
            "catalogos": catalogos,
            "status": "pending"
        }

        # Llamada a la función que envía el correo
        send_email_solicitud(solicitud_data)

        return jsonify({"message": "Solicitud creada exitosamente", "id": nuevo_id}), 201

    except Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# GET: Listar solicitudes (permite filtrar por ?status=pending|... y/o ?id=123)
@app.route('/api/solicitudes', methods=['GET'])
def obtener_solicitudes():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    status_param = request.args.get('status')
    id_param = request.args.get('id')
    cursor = conn.cursor(dictionary=True)
    try:
        base_query = "SELECT * FROM inventario_solicitudes"
        conditions = []
        values = []

        if status_param:
            conditions.append("status = %s")
            values.append(status_param)

        if id_param:
            conditions.append("id = %s")
            values.append(id_param)

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        base_query += " ORDER BY timestamp DESC"
        cursor.execute(base_query, tuple(values))
        rows = cursor.fetchall()
        return jsonify(rows), 200
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# PUT: Confirmar una solicitud (almacenar en inventario_solicitudes_conf + restar stock)
@app.route('/api/solicitudes/<int:solicitud_id>/confirm', methods=['PUT'])
def confirmar_solicitud(solicitud_id):
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    confirmador = data.get('confirmador')
    observaciones = data.get('observaciones', "")
    productos_finales = data.get('productos', {})  # {"merch_lapicero_clasico": 5, ...}

    if not confirmador:
        return jsonify({"error": "El campo 'confirmador' es requerido."}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor(dictionary=True)
    try:
        # 1. Verificar que la solicitud existe y su estado
        cursor.execute("SELECT status, grupo FROM inventario_solicitudes WHERE id = %s", (solicitud_id,))
        solicitud = cursor.fetchone()
        if not solicitud:
            return jsonify({"error": "La solicitud no existe."}), 404

        if solicitud['status'] != 'pending':
            return jsonify({
                "error": f"La solicitud no está pendiente (status actual: {solicitud['status']})."
            }), 400

        grupo = solicitud['grupo']
        conf_table = "inventario_solicitudes_conf"

        # 2. Insertar el registro de confirmación
        productos_json = json.dumps(productos_finales) if productos_finales else None
        insert_sql = f"""
            INSERT INTO {conf_table} (solicitud_id, confirmador, observaciones, productos, grupo)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (solicitud_id, confirmador, observaciones, productos_json, grupo))

        # 3. Actualizar el estado de la solicitud a 'confirmed'
        cursor.execute(
            "UPDATE inventario_solicitudes SET status = 'confirmed' WHERE id = %s",
            (solicitud_id,)
        )

        # (No se insertan registros negativos en inventario; el stock se calcula luego en /api/stock)

        conn.commit()
        return jsonify({"message": "Solicitud confirmada exitosamente"}), 200

    except Error as e:
        conn.rollback()
        print(f"Error en confirmación: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# GET: Obtener todas las confirmaciones de solicitudes
@app.route('/api/confirmaciones', methods=['GET'])
def obtener_confirmaciones():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor(dictionary=True)
    try:
        query = "SELECT * FROM inventario_solicitudes_conf ORDER BY timestamp DESC;"
        cursor.execute(query)
        confirmaciones = cursor.fetchall()
        return jsonify(confirmaciones), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# FUNCIÓN UTILITARIA PARA ASEGURAR COLUMNAS
# ---------------------------------------------------------
def ensure_column_exists(cursor, table_name, column_name):
    """
    Verifica si una columna existe en la tabla dada (table_name)
    y la crea si no existe. Apunta a la misma base de datos
    definida en DB_CONFIG.
    """
    try:
        db_name = DB_CONFIG['database']
        query_check = """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s;
        """
        cursor.execute(query_check, (db_name, table_name, column_name))
        (existe,) = cursor.fetchone()

        if existe == 0:
            print(f"Creando columna {column_name} en tabla {table_name}")
            query_alter = f"ALTER TABLE {table_name} ADD COLUMN `{column_name}` INT DEFAULT 0;"
            cursor.execute(query_alter)
            return True
        return False

    except Error as e:
        print(f"Error al verificar/crear columna {column_name}: {str(e)}")
        raise

# ---------------------------------------------------------
# EJECUTAR LA APLICACIÓN
# ---------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True)
