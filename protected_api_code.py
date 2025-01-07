from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
import os

# Loading environment variables from env file
load_dotenv()

app = Flask(__name__)

databases = {
    'db1': {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': 'consumer_law' 
    },
    'db2': {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': 'cyber_law'
    }
}

connection_pools = {
    db_name: pooling.MySQLConnectionPool(pool_name=f"{db_name}_pool", pool_size=10, **config)
    for db_name, config in databases.items()
}

def execute_query(pool, query):
    connection = pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

@app.route('/query', methods=['POST'])
def query_databases():
    data = request.json
    db_name = data.get('db_name')
    query = data.get('query')

    if db_name not in databases:
        return jsonify({"error": f"Database {db_name} not found"}), 400

    pool = connection_pools[db_name]
    try:
        results = execute_query(pool, query)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host=os.getenv('BACKEND_HOST'), port=int(os.getenv('BACKEND_PORT')), debug=True)
