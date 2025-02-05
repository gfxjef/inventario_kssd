import os
import json
import mysql.connector
from mysql.connector import Error
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)

# Configuración de CORS: permite tanto HTTP como HTTPS para los orígenes requeridos
CORS(app, resources={r"/*": {"origins": [
    "http://kossodo.estilovisual.com",
    "https://kossodo.estilovisual.com",
    "https://atusaludlicoreria.com"
]}})

# Configuración de la base de datos (se esperan variables de entorno)
DB_CONFIG = {
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'host': os.environ.get('MYSQL_HOST'),
    'database': os.environ.get('MYSQL_DATABASE'),
    'port': 3306
}

#######################################
# Creación de tablas en la base de datos
#######################################

# Columnas para las tablas de inventario de productos (merch)
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

# Nombres de las tablas de inventario de productos
TABLES_MERCH = [
    "inventario_merch_kossodo",
    "inventario_merch_kossomet"
]

# Query para crear las tablas de inventario de productos
table_queries = {}
for table in TABLES_MERCH:
    table_queries[table] = f"CREATE TABLE IF NOT EXISTS {table} ({columns_merch_str});"

# Tabla para las solicitudes de inventario
# Se almacenarán los productos solicitados en formato JSON en el campo 'productos'
solicitudes_columns = [
    "id INT AUTO_INCREMENT PRIMARY KEY",
    "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP",
    "solicitante VARCHAR(255)",
    "grupo VARCHAR(50)",
    "ruc VARCHAR(50)",
    "fecha_visita DATE",
    "cantidad_packs INT DEFAULT 0",
    "productos TEXT",
    "catalogos TEXT"
]
table_queries["inventario_solicitudes"] = f"CREATE TABLE IF NOT EXISTS inventario_solicitudes ({', '.join(solicitudes_columns)});"

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error de conexión: {e}")
        return None

def create_tables():
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

#######################################
# Endpoints para Inventario
#######################################

# GET: Obtener registros del inventario
# Ejemplo de uso: /api/inventario?tabla=kossodo o /api/inventario?tabla=kossomet
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
# Ejemplo de uso: /api/inventario?tabla=kossodo o /api/inventario?tabla=kossomet
@app.route('/api/inventario', methods=['POST'])
def agregar_inventario():
    tabla_param = request.args.get('tabla')
    if tabla_param not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Parámetro 'tabla' inválido. Use 'kossodo' o 'kossomet'."}), 400

    table_name = f"inventario_merch_{tabla_param}"
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    # Se esperan los siguientes campos (excepto id y timestamp, que se autogeneran)
    campos = [
        "responsable",
        "merch_lapiceros_normales",
        "merch_lapicero_ejecutivos",
        "merch_blocks",
        "merch_tacos",
        "merch_gel_botella",
        "merch_bolas_antiestres",
        "merch_padmouse",
        "merch_bolsa",
        "merch_lapiceros_esco",
        "observaciones"
    ]
    columnas = []
    valores = []
    for campo in campos:
        if campo in data:
            columnas.append(campo)
            valores.append(data[campo])
    if not columnas:
        return jsonify({"error": "No se han enviado campos válidos para insertar."}), 400

    placeholders = ", ".join(["%s"] * len(valores))
    columnas_str = ", ".join(columnas)
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


@app.route('/api/nuevo_producto', methods=['POST'])
def nuevo_producto():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    grupo = data.get('grupo')
    nombre_producto = data.get('nombre_producto')  # Nombre original para referencia
    columna = data.get('columna')  # Ej. "merch_lapicero_esco"
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
        # Verificar si la columna ya existe en la tabla usando information_schema
        query_check = """
            SELECT COUNT(*) FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s;
        """
        db_name = os.environ.get('MYSQL_DATABASE')
        cursor.execute(query_check, (db_name, table_name, columna))
        (existe,) = cursor.fetchone()

        if existe == 0:
            # La columna no existe; se procede a crearla
            query_alter = f"ALTER TABLE {table_name} ADD COLUMN {columna} INT DEFAULT 0;"
            cursor.execute(query_alter)
            conn.commit()

        # Insertar un registro en el inventario con la cantidad inicial en la columna nueva.
        # Nota: Si ya existen registros, quizá se desee actualizar en lugar de insertar; este ejemplo inserta un registro nuevo.
        query_insert = f"INSERT INTO {table_name} ({columna}) VALUES (%s);"
        cursor.execute(query_insert, (cantidad,))
        conn.commit()
        nuevo_id = cursor.lastrowid

        return jsonify({"message": "Nuevo producto agregado correctamente", "id": nuevo_id}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

#######################################
# Endpoint para Solicitudes
#######################################

# POST: Crear una nueva solicitud
# Se espera que el payload JSON incluya:
# - solicitante
# - grupo
# - ruc
# - fecha_visita
# - cantidad_packs
# - productos: (lista de productos solicitados)
# - catalogos
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
    productos = data.get('productos', [])  # Se espera un array de nombres de productos
    catalogos = data.get('catalogos', "")

    # Validación de campos requeridos
    if not solicitante or not grupo or not ruc or not fecha_visita:
        return jsonify({"error": "Faltan campos requeridos: solicitante, grupo, ruc, fecha_visita."}), 400

    # Convertir la lista de productos a un string JSON para almacenarlo
    productos_str = json.dumps(productos)

    query = """
        INSERT INTO inventario_solicitudes
        (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos, catalogos)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    values = (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos_str, catalogos)

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500

    cursor = conn.cursor()
    try:
        cursor.execute(query, values)
        conn.commit()
        nuevo_id = cursor.lastrowid
        return jsonify({"message": "Solicitud creada exitosamente", "id": nuevo_id}), 201
    except Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

#######################################
# Inicio de la aplicación
#######################################

if __name__ == '__main__':
    app.run(debug=True)
