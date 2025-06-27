# database setup module

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from . import db_config as db
import pandas as pd 

# === DB Configuration ===

DB_NAME=db.DB_NAME
DB_USER=db.DB_USER
DB_PASSWORD=db.DB_PASSWORD
DB_HOST=db.DB_HOST
DB_PORT=db.DB_PORT

# === DIR Config ===

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.join(base_dir, "data", "db_ready")

# Auxiliary manual local config 
""" 
DB_NAME = "moon_db"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "5432"
"""

# === 1. Connect to Default DB and Create the DB If Not Exists ===
def create_database():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {DB_NAME};")
            print("✅ Database created.")
        else:
            print("ℹ️ Database already exists.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating database: {e}")

# === 2. Connect to moon_db and Create Tables ===
def create_tables():
    ddl_statements = [

        """DROP TABLE IF EXISTS article CASCADE;""",

        """DROP TABLE IF EXISTS ingredient CASCADE;""",

        """DROP TABLE IF EXISTS inventory CASCADE;""",

        """DROP TABLE IF EXISTS recipe CASCADE;""",
        
        """DROP TABLE IF EXISTS demand CASCADE;""",

        """DROP VIEW IF EXISTS demand_summary CASCADE;"""

        """
        CREATE TABLE IF NOT EXISTS article (
            art_id SERIAL PRIMARY KEY,
            art_code VARCHAR(255) NOT NULL,
            art_name VARCHAR(255) NOT NULL,
            art_category VARCHAR(255),
            shelf_life FLOAT NULL
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS inventory (
            inv_id SERIAL PRIMARY KEY,
            art_code VARCHAR(255) NOT NULL,
            quantity INT NOT NULL,
            expiration_date DATE NOT NULL,
            batch_id VARCHAR(50)
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS recipe (
            recipe_id SERIAL PRIMARY KEY,
            recipe_name VARCHAR(255) NOT NULL
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS ingredient (
            ingredient_id SERIAL PRIMARY KEY,
            recipe_id INT NOT NULL,
            art_id INT NOT NULL,
            util_coeff INT NOT NULL,
            CONSTRAINT fk_recipe FOREIGN KEY (recipe_id) REFERENCES recipe(recipe_id) ON DELETE CASCADE,
            CONSTRAINT fk_article FOREIGN KEY (art_id) REFERENCES article(art_id) ON DELETE CASCADE
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS demand (
            demand_id SERIAL PRIMARY KEY,
            recipe_id INT NOT NULL,
            demand_q INT NOT NULL,
            demand_date DATE NOT NULL,
            CONSTRAINT fk_recipe FOREIGN KEY (recipe_id) REFERENCES recipe(recipe_id) ON DELETE CASCADE
        );
        """,

                """
        CREATE TABLE IF NOT EXISTS demand_suggested (
            demand_id SERIAL PRIMARY KEY,
            recipe_id INT NOT NULL,
            demand_q INT NOT NULL,
            demand_date DATE NOT NULL,
            CONSTRAINT fk_recipe FOREIGN KEY (recipe_id) REFERENCES recipe(recipe_id) ON DELETE CASCADE
        );
        """
    ]

    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        for stmt in ddl_statements:
            cur.execute(stmt)
        conn.commit()
        print("✅ Tables created or refreshed.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating tables: {e}")

# === 3. Load CSV Data into PostgreSQL Tables ===
def load_data():

    csv_files = {
        "article": "article_table.csv",
        "recipe": "recipe_table.csv",
        "ingredient": "ingredient_table.csv",
        "demand": "demand_table.csv",
        "inventory": "inventory_table.csv"
    }

    truncate_order = [
        "ingredient",
        "demand",
        "demand_suggested",
        "inventory",
        "recipe",
        "article"
    ]

    copy_order = [
        "article",
        "recipe",
        "ingredient",
        "demand"
        # "inventory" inventory needs to wait for the purchases to happen
    ]

    table_columns = {
        "article": "(art_id,art_code,art_name,art_category,shelf_life)",
        "recipe": "(recipe_id,recipe_name)",
        "ingredient": "(ingredient_id,recipe_id,art_id,util_coeff)",
        "demand": "(demand_id,recipe_id,demand_q,demand_date)",
        "inventory": "(inv_id,art_code,quantity,expiration_date,batch_id)"
    }

    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()

        # Truncate in reverse dependency order
        for table in truncate_order:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")

        # Load CSVs
        for table in copy_order:
            csv_path = os.path.join(db_dir, csv_files[table])
            with open(csv_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(f"COPY {table} {table_columns[table]} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
            print(f"✅ Loaded data into {table}.")

        conn.commit()
        cur.close()
        conn.close()
        print("🎯 All data loaded successfully.")

    except Exception as e:
        print(f"❌ Error loading data in {table}: {e}")

# === 4. Create View for Article-Level Demand Summary ===
def create_demand_summary_view():
    view_sql = """
    CREATE VIEW demand_summary AS
    WITH full_demand AS (
        SELECT * FROM demand
        JOIN recipe ON demand.recipe_id = recipe.recipe_id
        JOIN ingredient ON demand.recipe_id = ingredient.recipe_id
        JOIN article ON ingredient.art_id = article.art_id
    ), 
    clean_demand AS (
        SELECT
            demand_date,
            recipe_name,
            art_code,
            art_name,
            art_category,
            demand_q * util_coeff AS production_requirement
        FROM full_demand
        ORDER BY demand_date, recipe_name
    )
    SELECT 
        demand_date,
        art_code,
        SUM(production_requirement) AS art_demand
    FROM clean_demand
    GROUP BY demand_date, art_code
    ORDER BY 1, 2;
    """
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        cur.execute(view_sql)
        conn.commit()

        # Load view content into a DataFrame
        demand = pd.read_sql("SELECT * FROM demand_summary", conn)

        cur.close()
        conn.close()
        print("📈 View 'demand_summary' created successfully.")
        return demand
    except Exception as e:
        print(f"❌ Error creating view: {e}")
        return None

# === Run Setup ===
if __name__ == "__main__":
    create_database()
    create_tables()
    load_data()
    create_demand_summary_view()