import os
import json
import re
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
import google.genai as genai

from connect_db import get_engine
ENGINE: Engine = get_engine()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL")

# TOOL 1 – List Tables
def sql_db_list_tables():
    inspector = inspect(ENGINE)
    return {"tables": inspector.get_table_names()}


# TOOL 2 – Table Schema
def sql_db_schema(tables: str):
    inspector = inspect(ENGINE)
    all_tables = inspector.get_table_names()
    out = {}

    for table in [t.strip() for t in tables.split(",")]:
        if table not in all_tables:
            out[table] = {"error": f"Table '{table}' not found."}
            continue

        cols = inspector.get_columns(table)
        out[table] = [
            {"name": c["name"], "type": str(c["type"]), "nullable": c["nullable"]}
            for c in cols
        ]
    return out


# TOOL 3 – Query Checker
def sql_db_query_checker(sql: str):
    inspector = inspect(ENGINE)
    client = genai.Client(api_key=GOOGLE_API_KEY)

    schema = {
        t: [c["name"] for c in inspector.get_columns(t)]
        for t in inspector.get_table_names()
    }

    prompt = f"""
    You are a highly reliable SQL validation module designed specifically for **Microsoft SQL Server (T-SQL)** environments. 
    Your primary responsibility is to thoroughly inspect the provided SQL query and determine whether it is valid **T-SQL** 
    based on the given database schema. You must evaluate the query exactly as a strict SQL Server engine would—checking 
    identifiers, syntax, table/column existence, and T-SQL-compatible functions and clauses. Your analysis must be precise, 
    rule-based, and entirely grounded in the schema and SQL Server standards.

    You MUST follow these rules:

    1. **SQL DIALECT**
        - Assume the database is **Microsoft SQL Server**
        - Accept **only T-SQL syntax**
        - Do NOT use MySQL/Postgres/SQLite syntax such as:
            - LIMIT
            - backticks `table`
            - USING clause in JOIN
            - DATE('now')
            - strftime()
            - "::type" casts
            - ILIKE
            - RETURNING clause
            - Double-quoted identifiers ("column") — SQL Server does NOT use them by default.

    2. **VALIDATION LOGIC**
        - Check if all table names exist in the schema.
        - Check if all column names exist in their respective tables.
        - Check join conditions, where clauses, group by, order by, and functions for T-SQL compatibility.
        - Check for syntax issues or non-existent aliases.
        - If the SQL is correct according to the schema → mark valid = true.
        - If not → mark valid = false AND provide a corrected SQL version when possible.

    3. **CORRECTION RULES**
        - If a table or column name is wrong, replace it with the closest correct one from the schema.
        - If syntax resembles MySQL/Postgres, convert it to SQL Server style.
        - If the SQL is not fixable, set `"fixed_sql": null`.

    4. **ABOUT THE SCHEMA**
        - The SCHEMA JSON lists tables and their columns.
        - Column types, nullable flags, etc., are not included unless needed for reasoning.
        - Use the exact names as shown — case sensitive & no guessing beyond closest match.

    5. **OUTPUT FORMAT (IMPORTANT)**
        - You must return ONLY valid JSON.
        - No comments, no explanations outside JSON.
        - The JSON MUST contain:
            {{
          "valid": true/false,
          "message": "<very short explanation>",
          "fixed_sql": "<corrected SQL or null>"
            }}
    Here is the database schema (SQL Server):
    SCHEMA:
    {json.dumps(schema, indent=2)}
    Here is the SQL query to validate:
    QUERY:
    {sql}
    Now respond with ONLY the JSON described above. No extra text.
    """


    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt
    )

    text_response = response.text
    match = re.search(r"\{.*\}", text_response, re.S)
    if not match:
        return {"valid": False, "message": "Model returned no JSON.", "fixed_sql": None}

    return json.loads(match.group(0))



# TOOL 4 – Execute SQL
def sql_db_query(sql: str, limit: int = 5):
    s = sql.lstrip()
    try:
        if ENGINE.dialect.name == "mssql":
            if re.match(r'^\s*SELECT\s+TOP\b', s, re.IGNORECASE):
                final_sql = sql
            else:
                final_sql = re.sub(r'(?i)^\s*SELECT\b', f"SELECT TOP {limit}", sql, count=1)
        else:
            if re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
                final_sql = sql
            else:
                final_sql = f"{sql} LIMIT {limit}"

        with ENGINE.connect() as conn:
            rows = conn.execute(text(final_sql)).fetchall()
            return {"rows": [dict(r._mapping) for r in rows], "sql_executed": final_sql}

    except Exception as e:
        return {"error": str(e)}


TOOL_DOCS = {
    "sql_db_list_tables": {
        "description": (
            "Retrieve the list of all table names available in the currently connected SQL Server database. "
            "This tool ONLY returns table names. It does NOT return columns, types, indexes, or additional metadata. "
            "The output reflects the REAL tables that exist in the database engine at execution time."
        ),
        "parameters": {},
        "returns": {
            "tables": [
                "table_name_1",
                "table_name_2",
                "... additional table names ..."
            ]
        },
        "usage": (
            "Use this tool when generating SQL queries and you need to confirm which tables actually exist. "
            "Call this tool BEFORE referencing any table in a SQL query to avoid invalid table errors. "
            "This tool is commonly used as the first step when analyzing user questions about database structure."
        )
    },

    "sql_db_schema": {
        "description": (
            "Return the complete schema structure for one or more tables from the SQL Server database. "
            "The input must be a comma-separated string of table names. "
            "For each requested table, this tool returns a list of column definitions. "
            "If a table does NOT exist, the tool returns an error object for that table instead of column definitions."
        ),
        "parameters": {
            "tables": (
                "Comma-separated list of table names, e.g. 'customers,transactions'. "
                "Whitespace around names is automatically handled."
            )
        },
        "returns": {
            "<table_name>": [
                {
                    "name": "column_name",
                    "type": "SQL Server data type as string",
                    "nullable": True
                }
            ]
        },
        "usage": (
            "Use this tool to inspect table structures before generating SQL queries. "
            "This ensures the LLM uses the correct column names, avoids referencing non-existent fields, "
            "and generates valid JOIN conditions. "
            "This tool should be used whenever the LLM needs detailed schema information for accurate SQL generation."
        )
    },

    "sql_db_query_checker": {
        "description": (
            "Validate a SQL query using the actual database schema and SQL Server (T-SQL) rules. "
            "This tool checks: "
            "  • whether all referenced tables exist "
            "  • whether all referenced columns exist in the specified tables "
            "  • whether the query follows SQL Server T-SQL syntax conventions "
            "  • whether GROUP BY, ORDER BY, JOIN conditions, and expressions are valid "
            "  • whether the query mistakenly uses MySQL/Postgres/SQLite syntax (e.g., LIMIT, ::type, USING join) "
            "The tool returns: "
            "  • 'valid'→ boolean indicating whether the query is valid "
            "  • 'message'→ short description of what is correct or incorrect "
            "  • 'fixed_sql' → corrected SQL query (string) when an automatic correction is possible, else null "
            "This validation uses LLM reasoning combined with the real database schema."
        ),
        "parameters": {
            "query": (
                "The SQL query string to validate. "
                "Must be a SELECT or WITH query. "
                "Modification queries (INSERT/UPDATE/DELETE/DDL) are not permitted."
            )
        },
        "returns": {
            "valid": True,
            "message": "Short explanation of validation result.",
            "fixed_sql": "Corrected SQL string or null."
        },
        "usage": (
            "Always use this tool BEFORE executing any SQL query. "
            "If 'fixed_sql' is returned, it should be used instead of the original query. "
            "This tool ensures the LLM does not generate invalid SQL Server queries and helps maintain safety, "
            "correctness, and schema alignment."
        )
    },

    "sql_db_query": {
        "description": (
            "Execute a validated SQL query against the connected SQL Server database. "
            "This tool supports only read-only queries (SELECT or WITH). "
            "Before execution, the tool automatically injects a safety row limit to prevent large result sets: "
            "  • For SQL Server:  'SELECT TOP {limit}' is applied if not already present. "
            "The tool returns all retrieved rows as a list of dictionaries, where each dictionary maps "
            "column names to their corresponding values. "
            "The executed SQL is also returned for transparency and debugging."
        ),
        "parameters": {
            "query": (
                "The validated SQL query string. "
                "Must be read-only and must NOT modify the database."
            ),
            "limit": (
                "Maximum number of rows to return. Default = 5. "
                "The tool enforces this limit automatically if the query does not specify one."
            )
        },
        "returns": {
            "rows": [
                {"column_name": "value", "...": "... additional columns ..."}
            ],
            "sql_executed": "The final SQL string that was executed after adding TOP/LIMIT if needed."
        },
        "usage": (
            "Use this tool to run SELECT queries after they have been validated. "
            "This tool provides safe, predictable query execution and prevents full-table scans. "
            "Not allowed for INSERT, UPDATE, DELETE, or DDL statements."
        )
    }
}


def get_tool_docs_text():
    parts = []
    for name, doc in TOOL_DOCS.items():
        parts.append(
            f"### {name}\n"
            f"Description: {doc['description']}\n"
            f"Parameters: {doc.get('parameters', {})}\n"
            f"Returns: {doc.get('returns', {})}\n"
            f"Usage: {doc.get('usage', '')}\n"
        )
    return "\n".join(parts)




