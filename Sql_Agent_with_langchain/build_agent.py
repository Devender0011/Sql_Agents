import os
from dotenv import load_dotenv

load_dotenv()


from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.agents import create_agent

from langchain_google_genai import ChatGoogleGenerativeAI

# DB wrapper 
from connect_db import get_sql_database


if not os.getenv("API_KEY"):
    raise RuntimeError("GOOGLE_API_KEY is not set in the environment or .env file")

# Initialize Gemini model 
model = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite",
    temperature=0,
    api_key=os.getenv("API_KEY"),
)


db = get_sql_database()

# SQLDatabaseToolkit will create the tools: 
# sql_db_query, sql_db_schema, sql_db_list_tables, sql_db_query_checker
toolkit = SQLDatabaseToolkit(db=db, llm=model)
tools = toolkit.get_tools()


custom_descriptions = {
    "sql_db_query": (
        "Execute a SQL query on the connected database. "
        "This tool MUST be called only AFTER the query has been validated using sql_db_query_checker. "
        "Always run the validated SQL and return up to 5 rows unless the user explicitly requests more. "
        "Use this tool to actually retrieve data and produce final answers."
    ),

    "sql_db_schema": (
        "Return the schema for one or more tables, including column names and data types. "
        "Use this tool whenever you need to understand table structure before generating SQL, "
        "such as when you are unsure about column names or relationships."
    ),

    "sql_db_list_tables": (
        "List all tables available in the database. "
        "Use this as the first step when you are unsure which tables exist or which tables to reference in a SQL query."
    ),

    "sql_db_query_checker": (
        "Validate a SQL query for correctness, safety, and compatibility with the database dialect. "
        "Check table names, column names, SQL syntax, and logical consistency. "
        "**After this tool returns the validated SQL, the agent MUST call sql_db_query to execute it.**"
    ),
}


for t in tools:
    if t.name in custom_descriptions:
        t.description = custom_descriptions[t.name]


# System prompt
system_prompt = """
You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct {db.dialect} query,
validate it using sql_db_query_checker, THEN execute it with sql_db_query,
and finally return the answer. Limit results to at most 5 rows by default.
"""

agent = create_agent(
    model,
    tools,
    system_prompt=system_prompt,
)

def get_agent():
    return agent, model, db, tools

if __name__ == "__main__":
    get_agent()
    print("Agent ready.")