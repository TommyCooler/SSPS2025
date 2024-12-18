import os
from premsql.generators import Text2SQLGeneratorHF
import pandas as pd
import mysql.connector as mysql
import sqlparse
from google.cloud import translate_v2 as translate

# Google Cloud Service Account Configuration
#Add API Translation file json here
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

# Google Cloud Translator
def translate_text(text, target_language="en"):
    try:
        client = translate.Client()
        result = client.translate(text, target_language=target_language)
        return result["translatedText"]
    except Exception as e:
        print(f"Error for translate: {e}")
        return None

# Create Text2SQLGenerator
generator = Text2SQLGeneratorHF(
    model_or_name_or_path="premai-io/prem-1B-SQL",
    experiment_name="test_generators",
    device="cuda:0",
    type="test"
)

# Connect to database
def connect_database():
    try:
        conn = mysql.connect(
            host="localhost",
            user="root",
            password="12345",
            database="products"
        )
        return conn
    except mysql.Error as e:
        print(f"Error to connect database: {e}")
        raise

# Change the file direction
excel_file = "C://Users//tranc//Downloads//Sheet-Test_SQL1.xlsx"
if not os.path.exists(excel_file):
    raise FileNotFoundError(f"File is not exist: {excel_file}")

data = pd.read_excel(excel_file)
natural_language_queries = data["Natural Language Queries - Vietnamese"]
expected_sql_queries = data["Expected SQL Queries"]

# Queries processing and compute EX, EM
def process_queries(output_file, use_translation=False):
    conn = connect_database()
    cursor = conn.cursor()
    results = []
    total_queries = 0
    exact_match_count = 0
    execution_match_count = 0

    for i, query_condition in enumerate(natural_language_queries):
        if pd.isna(query_condition) or pd.isna(expected_sql_queries[i]):
            continue
        total_queries += 1

        # translate if need
        if use_translation:
            translated_query = translate_text(query_condition)
            if not translated_query:
                print(f"Cannot translate NLQ queries: {query_condition}")
                continue
            query_condition = translated_query

        # Generate SQL from model
        try:
            response = generator.generate(
                data_blob={
                    "prompt": f"""CREATE TABLE Laptops (
                                    Laptop_ID INT AUTO_INCREMENT PRIMARY KEY,
                                    Laptop_name VARCHAR(255) UNIQUE NOT NULL,
                                    Type VARCHAR(50),
                                    Price DECIMAL(10, 2),
                                    CPU VARCHAR(100),
                                    GPU VARCHAR(100),
                                    RAM VARCHAR(50),
                                    SSD VARCHAR(50),
                                    Description TEXT
                                );

                                CREATE TABLE Stores (
                                    Store_ID INT AUTO_INCREMENT PRIMARY KEY,
                                    Store_name VARCHAR(255) UNIQUE NOT NULL,
                                    Address VARCHAR(255),
                                    City VARCHAR(100),
                                    District VARCHAR(100)
                                );

                                CREATE TABLE Store_Laptop (
                                    ID INT AUTO_INCREMENT PRIMARY KEY,
                                    Store_ID INT NOT NULL,
                                    Laptop_ID INT NOT NULL,
                                    Quantity INT DEFAULT 0,
                                    Discount_Percentage DECIMAL(5,2),
                                    Last_Updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                    FOREIGN KEY (Store_ID) REFERENCES Stores(Store_ID) ON DELETE CASCADE,
                                    FOREIGN KEY (Laptop_ID) REFERENCES Laptops(Laptop_ID) ON DELETE CASCADE
                                );
                                question: only response one query for this NLQ by MySQL syntax- {query_condition}?"""
                },
                max_new_tokens=256
            )
            generated_sql = response.strip()
        except Exception as e:
            print(f"Error to generate SQL from NLQ: {query_condition}, error: {e}")
            generated_sql = None

        if not generated_sql:
            continue

        # Standardize SQL
        expected_sql = expected_sql_queries[i].strip()
        normalized_generated_sql = sqlparse.format(generated_sql, reindent=True, keyword_case='lower')
        normalized_expected_sql = sqlparse.format(expected_sql, reindent=True, keyword_case='lower')

        # Compute EM (Exact Match)
        is_sql_match = (normalized_generated_sql == normalized_expected_sql)
        if is_sql_match:
            exact_match_count += 1

        # Compute EX (Execution Match)
        is_result_match = False
        try:
            # Execution SQL generated
            cursor.execute(generated_sql)
            model_result = [tuple(row) for row in cursor.fetchall()]
        except mysql.Error as e:
            print(f"Error from execute generate SQL:\n{generated_sql}\nLỗi: {e}")
            model_result = None

        try:
            # Execution SQL expected
            cursor.execute(expected_sql)
            expected_result = [tuple(row) for row in cursor.fetchall()]
        except mysql.Error as e:
            print(f"Error from expect generate SQL:\n{expected_sql}\nLỗi: {e}")
            expected_result = None

        # Compare SQL
        if model_result is not None and expected_result is not None:
            is_result_match = (set(model_result) == set(expected_result))
            if is_result_match:
                execution_match_count += 1

        # Save to answer
        results.append({
            "Natural Language Query": query_condition,
            "Generated SQL": generated_sql,
            "Expected SQL": expected_sql,
            "SQL Match": "x" if is_sql_match else "",
            "Result Match": "x" if is_result_match else ""
        })

    # Compute EX và EM
    ex_score = (execution_match_count / total_queries) * 100 if total_queries else 0
    em_score = (exact_match_count / total_queries) * 100 if total_queries else 0

    # Save to file
    try:
        output_df = pd.DataFrame(results)
        output_df.to_excel(output_file, index=False)
        print(f"Save file complete {output_file}")
    except Exception as e:
        print(f"Error to save file: {e}")

    print(f"EX Score: {ex_score:.2f}%")
    print(f"EM Score: {em_score:.2f}%")
    return ex_score, em_score

# Run
if __name__ == "__main__":
    try:
        # Queries direction
        # change output_file to create new file and save file
        print("Queries direct processing...")
        ex_direct, em_direct = process_queries(output_file="Prem_direct1.xlsx", use_translation=False)

        # Queries translation API
        # change output_file to create new file and save file
        print("Queries translate processing..")
        ex_translated, em_translated = process_queries(output_file="Prem_translated1.xlsx", use_translation=True)

        print(f"Queries direction - EX: {ex_direct:.2f}%, EM: {em_direct:.2f}%")
        print(f"Queries translation - EX: {ex_translated:.2f}%, EM: {em_translated:.2f}%")
    except Exception as e:
        print(f"Error is occur: {e}")
