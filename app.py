import os
import json
import mysql.connector
from mysql.connector import Error
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)

# Configuración de CORS (ajusta los orígenes según necesites)
CORS(app, resources={r"/*": {"origins": ["*"]}})

# Configuración de la base de datos (se esperan variables de entorno)
DB_CONFIG = {
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'host': os.environ.get('MYSQL_HOST'),
    'database': os.environ.get('MYSQL_DATABASE'),
    'port': 3306
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error de conexión: {e}")
        return None

# ------------------------------------------------
# Endpoints de Inventario y Solicitudes (previos)
# ------------------------------------------------

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
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY timestamp DESC")
        rows = cursor.fetchall()
        return jsonify(rows), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/inventario', methods=['POST'])
def agregar_inventario():
    tabla_param = request.args.get('tabla')
    if tabla_param not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Parámetro 'tabla' inválido. Use 'kossodo' o 'kossomet'."}), 400
    table_name = f"inventario_merch_{tabla_param}"
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON"}), 400
    # Se asume que el payload tiene claves que corresponden a columnas
    columns = []
    values = []
    for key, value in data.items():
        columns.append(key)
        values.append(value)
    placeholders = ", ".join(["%s"] * len(values))
    columns_str = ", ".join(columns)
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500
    cursor = conn.cursor()
    try:
        cursor.execute(query, tuple(values))
        conn.commit()
        nuevo_id = cursor.lastrowid
        return jsonify({"message": "Registro agregado exitosamente", "id": nuevo_id}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/solicitud', methods=['POST'])
def crear_solicitud():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON"}), 400
    solicitante = data.get('solicitante')
    grupo = data.get('grupo')
    ruc = data.get('ruc')
    fecha_visita = data.get('fecha_visita')
    cantidad_packs = data.get('cantidad_packs', 0)
    catalogos = data.get('catalogos', "")
    if not (solicitante and grupo and ruc and fecha_visita):
        return jsonify({"error": "Faltan campos requeridos"}), 400
    productos_json = json.dumps(data.get('productos', []))
    query = """
    INSERT INTO inventario_solicitudes (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos, catalogos)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500
    cursor = conn.cursor()
    try:
        cursor.execute(query, (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos_json, catalogos))
        conn.commit()
        nuevo_id = cursor.lastrowid
        return jsonify({"message": "Solicitud creada exitosamente", "id": nuevo_id}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/nuevo_producto', methods=['POST'])
def nuevo_producto():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON"}), 400
    grupo = data.get('grupo')
    nombre_producto = data.get('nombre_producto')
    columna = data.get('columna')
    cantidad = data.get('cantidad', 0)
    if grupo not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Grupo inválido"}), 400
    if not (nombre_producto and columna):
        return jsonify({"error": "Faltan datos"}), 400
    table_name = f"inventario_merch_{grupo}"
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500
    cursor = conn.cursor()
    try:
        db_name = DB_CONFIG.get('database')
        query_check = """
          SELECT COUNT(*) FROM information_schema.COLUMNS
          WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """
        cursor.execute(query_check, (db_name, table_name, columna))
        (existe,) = cursor.fetchone()
        if existe == 0:
            query_alter = f"ALTER TABLE {table_name} ADD COLUMN {columna} INT DEFAULT 0"
            cursor.execute(query_alter)
            conn.commit()
        query_insert = f"INSERT INTO {table_name} ({columna}) VALUES (%s)"
        cursor.execute(query_insert, (cantidad,))
        conn.commit()
        nuevo_id = cursor.lastrowid
        return jsonify({"message": "Nuevo producto agregado", "id": nuevo_id}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/stock', methods=['GET'])
def obtener_stock():
    grupo = request.args.get('grupo')
    if grupo not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Grupo inválido"}), 400
    inventario_table = f"inventario_merch_{grupo}"
    stock_table = f"inventario_stock_{grupo}"
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500
    cursor = conn.cursor(dictionary=True)
    try:
        db_name = DB_CONFIG.get('database')
        query_cols = """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME LIKE 'merch\\_%'
        """
        cursor.execute(query_cols, (db_name, inventario_table))
        cols = [row['COLUMN_NAME'] for row in cursor.fetchall()]

        inventory_totals = {}
        for col in cols:
            query_sum = f"SELECT SUM(`{col}`) AS total FROM {inventario_table}"
            cursor.execute(query_sum)
            result = cursor.fetchone()
            total = result['total'] if result['total'] is not None else 0
            inventory_totals[col] = total

        request_totals = {col: 0 for col in cols}
        query_sol = "SELECT cantidad_packs, productos FROM inventario_solicitudes WHERE grupo = %s"
        cursor.execute(query_sol, (grupo,))
        sol_rows = cursor.fetchall()
        for row in sol_rows:
            cantidad = row['cantidad_packs'] if row['cantidad_packs'] is not None else 0
            try:
                productos_list = json.loads(row['productos']) if row['productos'] else []
            except Exception:
                productos_list = []
            for prod in productos_list:
                if prod in request_totals:
                    request_totals[prod] += cantidad
        stock = {}
        for col in cols:
            stock[col] = inventory_totals.get(col, 0) - request_totals.get(col, 0)

        create_stock_query = f"""
            CREATE TABLE IF NOT EXISTS {stock_table} (
                id INT PRIMARY KEY,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        cursor.execute(create_stock_query)
        conn.commit()

        query_cols_stock = """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME LIKE 'merch\\_%'
        """
        cursor.execute(query_cols_stock, (db_name, stock_table))
        stock_cols_existing = {row['COLUMN_NAME'] for row in cursor.fetchall()}

        for col in cols:
            if col not in stock_cols_existing:
                alter_query = f"ALTER TABLE {stock_table} ADD COLUMN {col} INT DEFAULT 0"
                cursor.execute(alter_query)
                conn.commit()

        columns_list = ', '.join([f"`{col}`" for col in cols])
        placeholders = ', '.join(['%s'] * len(cols))
        update_parts = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in cols])
        insert_query = f"""
            INSERT INTO {stock_table} (id, {columns_list})
            VALUES (1, {placeholders})
            ON DUPLICATE KEY UPDATE {update_parts}
        """
        cursor.execute(insert_query, tuple([stock[col] for col in cols]))
        conn.commit()

        cursor.execute(f"SELECT * FROM {stock_table} WHERE id = 1")
        stock_row = cursor.fetchone()
        return jsonify(stock_row), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/conf_solicitudes', methods=['GET'])
def obtener_solicitudes_conf():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM inventario_solicitudes")
        rows = cursor.fetchall()
        return jsonify(rows), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/conf_solicitudes', methods=['POST'])
def confirmar_solicitud():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON"}), 400
    solicitud_id = data.get("id")
    if not solicitud_id:
        return jsonify({"error": "Falta el id de la solicitud"}), 400

    solicitante = data.get("solicitante")
    grupo = data.get("grupo")
    ruc = data.get("ruc")
    fecha_visita = data.get("fecha_visita")
    cantidad_packs = data.get("cantidad_packs", 0)
    catalogos = data.get("catalogos", "")
    if not (solicitante and grupo and ruc and fecha_visita):
        return jsonify({"error": "Faltan campos requeridos"}), 400

    product_keys = [k for k in data.keys() if k.startswith("merch_")]

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Error de conexión a la base de datos"}), 500
    cursor = conn.cursor()
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS inventario_solicitudes_conf (
                id INT PRIMARY KEY,
                solicitante VARCHAR(255),
                grupo VARCHAR(50),
                ruc VARCHAR(50),
                fecha_visita DATE,
                cantidad_packs INT DEFAULT 0,
                catalogos TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        cursor.execute(create_table_query)
        conn.commit()

        db_name = os.environ.get('MYSQL_DATABASE')
        query_cols = """
            SELECT COLUMN_NAME 
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME LIKE 'merch\\_%'
        """
        cursor.execute(query_cols, (db_name, "inventario_solicitudes_conf"))
        existing_cols = {row[0] for row in cursor.fetchall()}

        for key in product_keys:
            if key not in existing_cols:
                alter_query = f"ALTER TABLE inventario_solicitudes_conf ADD COLUMN {key} INT DEFAULT 0"
                cursor.execute(alter_query)
                conn.commit()

        columns = ["id", "solicitante", "grupo", "ruc", "fecha_visita", "cantidad_packs", "catalogos"]
        values = [solicitud_id, solicitante, grupo, ruc, fecha_visita, cantidad_packs, catalogos]
        for key in product_keys:
            columns.append(key)
            values.append(data.get(key, 0))
        columns_str = ", ".join("`" + col + "`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_parts = ", ".join("`" + col + "` = VALUES(`" + col + "`)" for col in columns if col != "id")
        insert_query = f"""
            INSERT INTO inventario_solicitudes_conf ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_parts}
        """
        cursor.execute(insert_query, tuple(values))
        conn.commit()

        delete_query = "DELETE FROM inventario_solicitudes WHERE id = %s"
        cursor.execute(delete_query, (solicitud_id,))
        conn.commit()

        return jsonify({"message": "Solicitud confirmada y eliminada", "id": solicitud_id}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    app.run(debug=True)
