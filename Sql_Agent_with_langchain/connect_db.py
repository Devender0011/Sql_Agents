import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase

load_dotenv()

SERVER = os.environ.get("MSSQL_SERVER", r"ASCINLAP61389\SQLEXPRESS")
DATABASE = os.environ.get("MSSQL_DATABASE", "db")
DRIVER = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

USE_TRUSTED = True

if USE_TRUSTED:
    odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
    )
else:
    USER = os.environ.get("MSSQL_USER")
    PWD = os.environ.get("MSSQL_PASSWORD")
    if not USER or not PWD:
        raise RuntimeError("Set MSSQL_USER and MSSQL_PASSWORD in .env when using SQL auth.")
    odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USER};PWD={PWD};"
        "Encrypt=no;"
    )

connection_url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc)

def get_sql_database():
    return SQLDatabase.from_uri(connection_url) #engine = create_engine(connection_url)

if __name__ == "__main__":
    db = get_sql_database()
    print("Dialect:", db.dialect)
    print("Tables:", db.get_usable_table_names())














