import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

SERVER = os.environ.get("MSSQL_SERVER", r"ASCINLAP61389\SQLEXPRESS")
DATABASE = os.environ.get("MSSQL_DATABASE", "db")
DRIVER = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
TIMEOUT = int(os.environ.get("MSSQL_TIMEOUT", "30"))

def build_connection_string() -> str:
    from urllib.parse import quote_plus

    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
    )

    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"


def get_engine():
    conn_str = build_connection_string()
    try:
        engine = create_engine(
            conn_str,
            fast_executemany=True,
            connect_args={"timeout": TIMEOUT},
        )
        return engine
    except SQLAlchemyError as e:
        print("DATABASE CONNECTION FAILED")
        print("Connection string:", conn_str)
        print("Error:", e)
        raise