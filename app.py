import os
import json
import mysql.connector
from mysql.connector import Error
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)

# Configuración de CORS (ajusta orígenes según tu necesidad)
CORS(app, resources={r"/*": {"origins": [
    "http://kossodo.estilovisual.com",
    "https://kossodo.estilovisual.com",
    "https://atusaludlicoreria.com"
]}})

# Configuración de la base de datos
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

# Tablas de inventario para Kossodo y Kossomet
TABLES_MERCH = [
    "inventario_merch_kossodo",
    "inventario_merch_kossomet"
]

table_queries = {}
for table in TABLES_MERCH:
    table_queries[table] = f"CREATE TABLE IF NOT EXISTS {table} ({columns_merch_str});"

# Tabla de solicitudes (con status='pending' por defecto)
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
    "merch_lapiceros_normales INT DEFAULT 0",
    "merch_lapicero_ejecutivos INT DEFAULT 0",
    "merch_blocks INT DEFAULT 0",
    "merch_tacos INT DEFAULT 0",
    "merch_gel_botella INT DEFAULT 0",
    "merch_bolas_antiestres INT DEFAULT 0",
    "merch_padmouse INT DEFAULT 0",
    "merch_bolsa INT DEFAULT 0",
    "merch_lapiceros_esco INT DEFAULT 0",
    "FOREIGN KEY (solicitud_id) REFERENCES inventario_solicitudes(id)"
]
table_queries["inventario_solicitudes_conf"] = f"CREATE TABLE IF NOT EXISTS inventario_solicitudes_conf ({', '.join(conf_columns)});"


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
# Endpoints para Inventario (Merch)
#######################################

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

    # Campos esperados en data
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


# POST: Agregar un nuevo tipo de producto (nueva columna) en la tabla + insertar un registro
@app.route('/api/nuevo_producto', methods=['POST'])
def nuevo_producto():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    grupo = data.get('grupo')
    nombre_producto = data.get('nombre_producto')  # Nombre original
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
        db_name = os.environ.get('MYSQL_DATABASE')
        query_check = """
            SELECT COUNT(*) FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s;
        """
        cursor.execute(query_check, (db_name, table_name, columna))
        (existe,) = cursor.fetchone()

        if existe == 0:
            # Crear la columna si no existe
            query_alter = f"ALTER TABLE {table_name} ADD COLUMN {columna} INT DEFAULT 0;"
            cursor.execute(query_alter)
            conn.commit()

        # Insertar un registro con la cantidad inicial
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


# GET: Obtener el stock calculado y actualizar inventario_stock_{grupo}
@app.route('/api/stock', methods=['GET'])
def obtener_stock():
    import json
    
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

        # 3. Calcular total solicitado por producto
        request_totals = {col: 0 for col in cols}
        # Solo solicitudes de este grupo
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

        # 4. Stock = inventario - solicitudes
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

        # Agregar columnas que falten
        for col in cols:
            if col not in stock_cols_existing:
                alter_query = f"ALTER TABLE {stock_table} ADD COLUMN {col} INT DEFAULT 0;"
                cursor.execute(alter_query)
                conn.commit()

        # Actualizar (o insertar) la fila con id=1 en la tabla de stock
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

        # 6. Retornar la fila actualizada
        cursor.execute(f"SELECT * FROM {stock_table} WHERE id = 1;")
        stock_row = cursor.fetchone()
        return jsonify(stock_row), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

#######################################
# Endpoints para Solicitudes
#######################################

# POST: Crear una nueva solicitud
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
        # Ejecuta siempre el CREATE TABLE IF NOT EXISTS para inventario_solicitudes
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

        # Inserta la nueva solicitud
        insert_sql = """
            INSERT INTO inventario_solicitudes
            (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos, catalogos)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        values = (solicitante, grupo, ruc, fecha_visita, cantidad_packs, productos_str, catalogos)
        cursor.execute(insert_sql, values)
        conn.commit()

        nuevo_id = cursor.lastrowid
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
    productos_finales = data.get('productos', {})

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
        inv_table = f"inventario_merch_{grupo}"

        # 2. Asegurar que todas las columnas existan en ambas tablas
        for col_name in productos_finales.keys():
            # Solo creamos columnas que empiecen con merch_ (según tu lógica)
            if col_name.startswith('merch_'):
                ensure_column_exists(cursor, conf_table, col_name)
                ensure_column_exists(cursor, inv_table, col_name)
        
        # Hacemos commit tras crear (si es que se crearon)
        conn.commit()

        # 3. Insertar el registro de confirmación
        base_cols = ['solicitud_id', 'confirmador', 'observaciones']
        prod_cols = list(productos_finales.keys())  # 'merch_xxxx'
        all_cols = base_cols + prod_cols

        cols_str = ", ".join(all_cols)  # solicitud_id, confirmador, observaciones, merch_xxx, ...
        placeholders = ", ".join(["%s"] * len(all_cols))
        insert_sql = f"INSERT INTO {conf_table} ({cols_str}) VALUES ({placeholders})"

        valores = [solicitud_id, confirmador, observaciones] + [
            productos_finales[col] for col in prod_cols
        ]
        cursor.execute(insert_sql, tuple(valores))

        # 4. Actualizar el estado de la solicitud a 'confirmed'
        cursor.execute(
            "UPDATE inventario_solicitudes SET status = 'confirmed' WHERE id = %s",
            (solicitud_id,)
        )

        # 5. Registrar la salida en inventario (insertamos valores negativos)
        if productos_finales:
            inv_cols = ['responsable'] + list(productos_finales.keys())
            inv_vals = [f"Confirmación {solicitud_id}"] + [
                -abs(qty) for qty in productos_finales.values()
            ]

            inv_cols_str = ", ".join(inv_cols)
            inv_placeholders = ", ".join(["%s"] * len(inv_vals))
            inv_sql = f"INSERT INTO {inv_table} ({inv_cols_str}) VALUES ({inv_placeholders})"
            cursor.execute(inv_sql, tuple(inv_vals))

        conn.commit()
        return jsonify({"message": "Solicitud confirmada exitosamente"}), 200

    except Error as e:
        conn.rollback()
        print(f"Error en confirmación: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()






def ensure_column_exists(cursor, table_name, column_name):
    """
    Verifica si una columna existe en la tabla y la crea si no existe.
    """
    try:
        # ¡Usar DB_CONFIG['database'] en lugar de os.environ.get!
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



#######################################
# Inicio de la aplicación
#######################################
if __name__ == '__main__':
    app.run(debug=True)
