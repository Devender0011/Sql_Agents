SQLAGENTFINAL/
│
├── __pycache__/
│
├── templates/
│   └── viz_index.html
│        Jinja2 HTML template for the web UI.
│        - Question input form
│        - SQL display
│        - Table rendering
│        - Plotly chart embedding
│        - Query history sidebar
│
├── .env
│     Environment configuration file.
│     Stores:
│       - GOOGLE_API_KEY
│       - GEMINI_MODEL
│       - Database credentials / connection strings
│     Loaded via python-dotenv.
│
├── connect_db.py
│     Database connection layer.
│     - Creates SQLAlchemy / pyodbc engine
│     - Manages connection pooling
│     - Used by sql_tools.py and fallback execution in web_app.py
│
├── flow_diagram.md
│     High-level architecture & execution flow.
│     - User → SQL Agent → Validation → DB → Result
│     - Simple vs Complex request pipelines
│     - LLM interaction boundaries
│
├── history_utils.py
│     Query history management.
│     - load_history()
│     - add_history_entry()
│     - save_history()
│     Persists question + SQL metadata to JSON.
│
├── query_history.json
│     Persistent query log.
│     Stores:
│       - user questions
│       - generated_sql
│       - validated_sql
│       - is_complex flag
│     Used for UI history + fallback execution.
│
├── main.py
│     Core application entry point (API / CLI use).
│     - Accepts user input
│     - Calls process_user_request() from sql_agent.py
│     - Returns structured JSON response
│
├── requirements.txt
│     Python dependencies:
│       - fastapi
│       - uvicorn
│       - google-genai
│       - pandas
│       - plotly
│       - sqlalchemy
│       - pyodbc / pymssql
│       - python-dotenv
│
├── sql_agent.py
│     CORE INTELLIGENCE LAYER
│
│     Responsibilities:
│       - Natural language → SQL generation (Gemini)
│       - Schema-aware prompting
│       - Query validation & repair loops
│       - Complex request decomposition
│       - Safe execution enforcement
│
│     Key functions:
│       - process_user_request()
│       - is_complex_request()
│       - nl_to_sql()
│       - handle_complex_request()
│       - _split_request_with_llm()
│       - _combine_tabular_results()
│       - _basic_execute_safety()
│       - extract_json_from_text()
│       - _call_gemini()
│
├── sql_tools.py
│     Database utility & tooling layer.
│
│     Functions:
│       - sql_db_list_tables()
│       - sql_db_schema()
│       - sql_db_query_checker()
│       - sql_db_query()
│       - get_tool_docs_text()
│
│     Used exclusively by sql_agent.py.
│
├── web_app.py
│     FastAPI web interface.
│
│     Features:
│       - Natural language query input
│       - SQL Agent execution
│       - Pandas DataFrame rendering
│       - Auto chart selection (Plotly)
│       - CSV download
│       - Query history replay
│       - Fallback SQL execution on agent failure
│
│     Routes:
│       - GET  /              → UI
│       - POST /ask           → Run NL → SQL
│       - GET  /download_csv  → Export results
│       - GET  /_envcheck     → Debug env vars
│
├── tree_structure.md
│     Project structure documentation (this file).
│     - Explains responsibility of each module
│     - Useful for interviews, reviews, handoff
│
└── README.md (optional but recommended)
      Project overview, setup instructions, and usage.
