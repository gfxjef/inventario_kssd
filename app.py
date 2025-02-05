import os
import mysql.connector
from mysql.connector import Error
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)

# Configuración de CORS para los orígenes permitidos
CORS(app, resources={r"/*": {"origins": [
    "https://atusaludlicoreria.com",
    "https://kossodo.estilovisual.com"
]}})

# Configuración de la base de datos (se esperan variables de entorno)
DB_CONFIG = {
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'host': os.environ.get('MYSQL_HOST'),
    'database': os.environ.get('MYSQL_DATABASE'),
    'port': 3306
}

# Definición de las tablas y sus columnas
TABLES = ['inventario_merch_kossodo', 'inventario_merch_kossomet']
COLUMNS = [
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
    columnas_sql = ", ".join(COLUMNS)
    for table in TABLES:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ({columnas_sql});"
        try:
            cursor.execute(create_table_query)
            conn.commit()
            print(f"Tabla '{table}' verificada/creada correctamente.")
        except Error as e:
            print(f"Error al crear la tabla {table}: {e}")
    cursor.close()
    conn.close()

# Ejecutar la creación de tablas al iniciar la aplicación
create_tables()

# Endpoint para obtener registros de un inventario específico.
# Se debe enviar en la query string el parámetro 'tabla' con valor 'kossodo' o 'kossomet'
@app.route('/api/inventario', methods=['GET'])
def obtener_inventario():
    tabla_param = request.args.get('tabla')
    if tabla_param not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Parámetro 'tabla' inválido. Use 'kossodo' o 'kossomet'."}), 400

    table_name = f"inventario_merch_{'kossodo' if tabla_param=='kossodo' else 'kossomet'}"
    
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

# Endpoint para agregar un nuevo registro a un inventario específico.
# Se debe enviar en la query string el parámetro 'tabla' y en el body (JSON) los datos
@app.route('/api/inventario', methods=['POST'])
def agregar_registro():
    tabla_param = request.args.get('tabla')
    if tabla_param not in ['kossodo', 'kossomet']:
        return jsonify({"error": "Parámetro 'tabla' inválido. Use 'kossodo' o 'kossomet'."}), 400
    table_name = f"inventario_merch_{'kossodo' if tabla_param=='kossodo' else 'kossomet'}"
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "No se proporcionaron datos en formato JSON."}), 400

    # Se definen los campos que se esperan (exceptuando id y timestamp, que se autogeneran)
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
    
    # Extraer solo los campos necesarios y preparar los valores
    valores = []
    columnas = []
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

if __name__ == '__main__':
    app.run(debug=True)
