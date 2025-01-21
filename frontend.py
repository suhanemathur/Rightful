from flask import Flask, render_template, request, jsonify
import asyncio
from backendlogic import (
    query_llama_model,
    extract_sql_query,
    detect_query_type,
    find_table_database,
    query_laptop,
    handle_cross_database_query,
    generate_schema_context
)
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')  

@app.route('/search', methods=['POST'])
async def search():
    try:
        user_query = request.form['query']  

        GROQ_API_KEY = os.getenv("GROQ_API_KEY")  
        GROQ_MODEL_NAME = os.getenv("GROQ_MODEL_NAME") 
        schema_context = await generate_schema_context()
        llm_response = query_llama_model(GROQ_API_KEY, GROQ_MODEL_NAME, user_query, schema_context)
        response_or_sql = extract_sql_query(llm_response)

        if response_or_sql.startswith("SELECT"): 
            query_type, table_names, join_condition, join_type = detect_query_type(response_or_sql)

            if query_type == "single":
                db_name, laptop_url = await find_table_database(table_names[0])
                if not db_name:
                    return jsonify({"error": f"Table '{table_names[0]}' not found in any database."})
                result = await query_laptop(laptop_url, db_name, response_or_sql)
                if result and "data" in result and result["data"]:
                    return jsonify({"result": result["data"]})
                else:
                    return jsonify({"error": "No data found or query execution error."})

            elif query_type == "cross":
                result_df = await handle_cross_database_query(table_names, join_type, join_condition)
                if result_df is not None:
                    return jsonify({"result": result_df.to_dict(orient='records')})
                else:
                    return jsonify({"error": "No data found for cross-database query."})

            else:
                return jsonify({"error": "Unsupported query type detected."})

        else:
            return jsonify({"response": response_or_sql})

    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == '__main__':
    app.run(
        host=os.getenv("FRONTEND_HOST", "192.168.1.8"), 
        port=int(os.getenv("FRONTEND_PORT", 5001)),   
        debug=True
    )
