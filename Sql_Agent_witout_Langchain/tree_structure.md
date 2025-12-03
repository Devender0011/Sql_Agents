project-root/
│
├── .env
│      Stores environment variables such as:
│      - Database credentials
│      - Google API keys (GEMINI_MODEL / GOOGLE_API_KEY)
│      - Connection strings
│      Loaded by python-dotenv.
│
├── connect_db.py
│      Database connection handler.
│      - Creates SQL Server engine/connection pool
│      - Manages retries / error handling
│      - Ensures efficient DB interactions for sql_tools.py
│
├── flow_diagram.md
│      High-level architecture documentation.
│      - ASCII pipeline diagrams
│      - Request flow (User → Agent → DB → Result)
│      - Ideal for engineering design reviews
│
├── main.py
│      Application entry point.
│      - Receives user input (API or CLI)
│      - Calls process_user_request() from sql_agent.py
│      - Returns JSON response
│      This is what your frontend/UI will call.
│
├── requirements.txt
│      Python dependency list:
│      - google-genai (Gemini API)
│      - pandas
│      - pyodbc / pymssql
│      - python-dotenv
│      - fastapi / flask (if used)
│      Used to recreate environment quickly.
│
├── sql_agent.py
│      ****** CORE LOGIC OF THE ENTIRE PROJECT ******
│
│      Contains:
│      - process_user_request()     → main orchestration
│      - is_complex_request()       → detects multi-part queries
│      - nl_to_sql()                → LLM prompt → SQL generator
│      - handle_complex_request()   → splitter + multi-query handler
│      - _split_request_with_llm()  → LLM decides how to break tasks
│      - _combine_tabular_results() → merge data using pandas or LLM
│      - _call_gemini()             → LLM invocation
│      - _basic_execute_safety()    → prevents harmful SQL
│      - extract_json_from_text()   → ensures clean JSON from LLM
│      
│      This file implements:
│      NLP → SQL → Validation → Safe execution → Final response
│
│
├── sql_tools.py
       SQL utility functions — used by sql_agent.py.
         - sql_db_list_tables()       → get all tables
         - sql_db_schema()            → get schema of tables
         - sql_db_query_checker()     → validate SQL, fix errors
         - sql_db_query()             → execute final SQL safely
         - get_tool_docs_text()       → used inside LLM prompt
