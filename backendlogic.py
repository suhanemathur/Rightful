import asyncio
import httpx
import pandas as pd
import re
import sqlparse
from rapidfuzz import fuzz, process
import requests
import os
from dotenv import load_dotenv
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'  

load_dotenv()

import tensorflow as tf
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

laptops = [
    {"url": os.getenv("LAPTOP1_URL"), "dbs": ["familylaws"]},  
    {"url": os.getenv("LAPTOP2_URL"), "dbs": ["consumer_law", "cyber_law"]}, 
]

value_synonyms = {"All India": "India", "Pan-India": "India", "Nationwide": "India"}

def normalize_values(df, column_name):
    if column_name in df.columns:
        df[column_name] = df[column_name].replace(value_synonyms)
    return df

def generate_query_with_filters(table_name, filters):
    """
    Generate a query with filters applied to minimize data transfer.
    """
    where_clause = " AND ".join([f"{col} = '{val}'" for col, val in filters.items()])
    return f"SELECT * FROM {table_name} WHERE {where_clause};"


GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = os.getenv("GROQ_API_URL")
GROQ_MODEL_NAME = os.getenv("GROQ_MODEL_NAME")

def query_llama_model(api_key, model, user_input, schema_context):
    prompt = f"""First understand the entire prompt and respond accordingly:
1. Analyse the user's query: "{user_input}".
2. Compare and analyse it with the given schema information: {schema_context}.
3. Follow these rules based on your analysis:
    - If the query can be answered by databases in the schema,strictly perform *TASK 1*.
    - If the query is abstract or does NOT relate to the schema, perform *TASK 2*.
    -DONT BLINDLY GIVE ANSWERS, UNDERSTAND THE INPUT.

*TASK 1*: Generate an SQL query that adheres to the following rules:
    - Generate the SQL query only based on the schema.
    - IMPORTANT: Do not add any explanations, comments, or extra text.
    - Wrap the query with the delimiters <SQL> and </SQL>.
    - The query must be on a single line with no indentation or code block formatting.
    - Use only table names without including database names (e.g., convert consumer_law.consumer_protection to consumer_protection).
    - Ensure column names match exactly as per the schema.
    - End the query with a semicolon (;).
    - Example output: <SQL>SELECT * FROM consumer_protection;</SQL>
    *NOTE*: Always check the following in SQL queries:
   - Syntax validity.
   - Logical correctness (e.g., single-value vs. multi-value subquery handling).
   - Adherence to schema structure (e.g., column and table names).

*TASK 2*: If the query does not relate to the schema:
    - Answer as a large language model in plain text.
    - Provide a clear, detailed, and expert explanation or response to the user's query.

*IMPORTANT*: Always ensure your response matches the task based on the user's query and schema context. Do not mix SQL output and plain text in a single response.

"""

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "model": model
    }
    try:
        response = requests.post(GROQ_API_URL, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error querying Groq LLaMA model: {e}")
        return None

def normalize_column_name(column_name):
    return column_name.lower().replace("_", "").replace(" ", "").replace(".", "").strip(";")

def detect_query_type(query):
    query_lower = query.lower()
    if "select" in query_lower and "from" in query_lower:
        if "join" in query_lower:  
            parts = query_lower.split("from")[1].split("join")
            table_names = [part.strip().split()[0].strip(";") for part in parts]
            join_type = parse_join_type(query)
            join_condition = parse_join_condition(query)
            return "cross", table_names, join_condition, join_type
        else:  
            table_name = query_lower.split("from")[1].strip().split()[0].strip(";")
            return "single", [table_name], None, None
    return None, [], None, None


def parse_join_type(query):
    match = re.search(r'\b(INNER|LEFT|RIGHT|FULL)\s+JOIN\b', query, re.IGNORECASE)
    return match.group(1).upper() if match else "INNER"


def parse_join_condition(query):
    match = re.search(r'ON\s+(.+?)(?:\s+(WHERE|GROUP BY|ORDER BY|LIMIT|;)|$)', query, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

async def query_laptop(laptop_url, db_name, query):
    async with httpx.AsyncClient() as client:
        try:
            print(f"Sending Query: '{query}' to Database: {db_name} at URL: {laptop_url}")  
            response = await client.post(laptop_url, json={"db_name": db_name, "query": query})
            response.raise_for_status()
            print(f"Query Successful, Response: {response.json()}")  
            return {"db_name": db_name, "data": response.json()}
        except httpx.RequestError as e:
            print(f"Request error: {str(e)}")
            return {"db_name": db_name, "error": f"Request error: {str(e)}"}
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
            return {"db_name": db_name, "error": f"HTTP error: {e.response.status_code} - {e.response.text}"}


async def fetch_tables(laptop_url, db_name):
    db_mapping = {
        "consumer_law": "db1", 
        "cyber_law": "db2",
        "familylaws":"db3"
    }
    config_db_name = db_mapping.get(db_name, db_name) 

    query = "SHOW TABLES;"
    result = await query_laptop(laptop_url, config_db_name, query)
    if "error" not in result:
        first_row = result["data"][0]
        column_name = list(first_row.keys())[0] 
        return [row[column_name] for row in result["data"]]
    return []


async def find_table_database(table_name):
    db_mapping = {
        "consumer_law": "db1",
        "cyber_law": "db2",
        "familylaws":"db3"
    }

    for laptop in laptops:
        for db_name in laptop["dbs"]:
            mapped_db_name = db_mapping.get(db_name, db_name)
            tables_in_db = await fetch_tables(laptop["url"], mapped_db_name)
            print(f"Checking Table: {table_name} in Database: {mapped_db_name}, Tables Found: {tables_in_db}")  # for Debugging
            if table_name in tables_in_db:
                return mapped_db_name, laptop["url"]
    return None, None


async def handle_cross_database_query(table_names, join_type, on_condition):
    table_data = {}
    for table_name in table_names:
        db_name, laptop_url = await find_table_database(table_name)
        if not db_name:
            print(f"Error: Table '{table_name}' not found in any database.")
            return None
        query = f"SELECT * FROM {table_name};"
        result = await query_laptop(laptop_url, db_name, query)
        if result and "data" in result and result["data"]:
            df = pd.DataFrame(result["data"])
            df.columns = [normalize_column_name(col) for col in df.columns]
            table_data[table_name] = df
        else:
            print(f"No data found for table {table_name}.")
            return None

    for table_name, df in table_data.items():
        table_data[table_name] = normalize_values(df, "jurisdiction")  

    merged_df = None
    for i, table_name in enumerate(table_names):
        if i == 0:
            merged_df = table_data[table_name]
        else:
            next_df = table_data[table_name]
            join_columns = [normalize_column_name(col.strip().split('.')[1]) for col in on_condition.split('=')]

            if join_columns[0] in merged_df.columns and join_columns[1] in next_df.columns:
                merged_df = pd.merge(
                    merged_df, next_df, left_on=join_columns[0], right_on=join_columns[1], how=join_type.lower()
                )
    return merged_df



async def generate_schema_context():
    schema_context = [
        {
            "database": "familylaws",
            "tables": [
                {"name": "guardians_and_wards_act_1980", "columns": ["Section_Number", "Chapter", "Description", "Rights", "Jurisdiction"]},
                {"name": "hindu_marriage_act_1955", "columns": ["Section_Number", "Chapter", "Description", "Rights", "Jurisdiction", "Keywords"]},
                {"name": "muslim_personal_law_shariat_application_act_1937", "columns": ["Section_Number", "Chapter", "Description", "Rights", "Jurisdiction", "Keywords"]},
            ]
        },
        {
            "database": "consumer_law",
            "tables": [
                {"name": "consumer_protection", "columns": ["section_number", "title", "description", "keywords", "jurisdiction", "rights"]},
                {"name": "food_safety", "columns": ["section_number", "chapter", "description", "rights", "keywords", "jurisdiction"]},
                {"name": "legal_metrology", "columns": ["section_number", "description", "rights", "jurisdiction", "keywords"]},
            ]
        },
        {
            "database": "cyber_law",
            "tables": [
                {"name": "ipc_cybersecurity_sections", "columns": ["section_number", "description", "keywords", "jurisdiction", "rights"]},
                {"name": "it_act_2000", "columns": ["section_number", "description", "keywords", "jurisdiction", "rights"]},
                {"name": "personal_data_bill_2019", "columns": ["section", "chapter", "description", "rights", "keywords", "jurisdiction"]},
            ]
        }
    ]

    schema_text = []
    for db in schema_context:
        schema_text.append(f"Database: {db['database']}")
        for table in db["tables"]:
            schema_text.append(f"  Table: {table['name']}")
            schema_text.append(f"    Columns: {', '.join(table['columns'])}")
    return "\n".join(schema_text)


def extract_sql_query(llm_response):
    sql_start = llm_response.find("<SQL>")
    sql_end = llm_response.find("</SQL>")
    if sql_start != -1 and sql_end != -1:
        query = llm_response[sql_start + len("<SQL>"):sql_end].strip()
        if not query.endswith(";"):
            query += ";"
        
        query_parts = query.split()
        if "from" in query_parts:
            table_index = query_parts.index("from") + 1
            if "." in query_parts[table_index]:
                query_parts[table_index] = query_parts[table_index].split(".")[1]
        return " ".join(query_parts)
    else:
        print("No SQL query found in LLM response.")
        return llm_response.strip()



import os

async def store_result_in_csv(data, file_name="query_result.csv"):
    """
    Stores the provided data in a CSV file.
    """
    try:
        if not data:
            print("No data available to store.")
            return

        df = pd.DataFrame(data)

        file_path = os.path.join(os.getcwd(), file_name)
        df.to_csv(file_path, index=False)
        print(f"Data stored successfully in {file_path}")
    except Exception as e:
        print(f"Error while storing data in CSV: {e}")


async def main():
    schema_context = await generate_schema_context()
    print("Schema Context Generated")
    print("Distributed Query Interface with LLM Integration")
    print("=================================================")
    while True:
        user_input = input("Enter your query (or type 'exit' to quit): ").strip()
        if user_input.lower() == "exit":
            print("Exiting the interface. Goodbye!")
            break

        print("Processing User Input")
        try:
            llm_response = query_llama_model(GROQ_API_KEY, GROQ_MODEL_NAME, user_input, schema_context)
            print(f"LLM Response: {llm_response}")

            sql_query = extract_sql_query(llm_response)
            if not sql_query:
                print("No valid SQL query found. LLM Response:")
                print(llm_response)
                continue

            print(f"Extracted SQL Query: {sql_query}")

            query_type, table_names, join_condition, join_type = detect_query_type(sql_query)
            print(f"Query Type: {query_type}, Table Names: {table_names}")

            if query_type == "single":
                db_name, laptop_url = await find_table_database(table_names[0])
                if not db_name:
                    raise ValueError(f"Table '{table_names[0]}' not found in any database.")
                print(f"Querying Database: {db_name} at {laptop_url}")
                result = await query_laptop(laptop_url, db_name, sql_query)
                if result and "data" in result and result["data"]:
                    df = pd.DataFrame(result["data"])
                    print("Query Result:")
                    print(df.head(5))
                    
                    save_choice = input("Do you want to save the result to a CSV file? (yes/no): ").strip().lower()
                    if save_choice == "yes":
                        file_name = input("Enter the desired file name (default: query_result.csv): ").strip()
                        file_name = file_name if file_name else "query_result.csv"
                        await store_result_in_csv(result["data"], file_name)
                else:
                    print(f"Query Execution Error: {result.get('error', 'No data returned')}")
                    raise ValueError("No data returned from query.")
            elif query_type == "cross":
                print("Handling Cross-Database Query")
                result_df = await handle_cross_database_query(table_names, join_type, join_condition)
                if result_df is not None:
                    print("Query Result:")
                    print(result_df.head(5))
                    
                    save_choice = input("Do you want to save the result to a CSV file? (yes/no): ").strip().lower()
                    if save_choice == "yes":
                        file_name = input("Enter the desired file name (default: query_result.csv): ").strip()
                        file_name = file_name if file_name else "query_result.csv"
                        result_df.to_csv(file_name, index=False)
                        print(f"Data stored successfully in {file_name}")
                else:
                    raise ValueError("No data found for cross-database query.")
            else:
                print("Unsupported query type detected.")
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())

