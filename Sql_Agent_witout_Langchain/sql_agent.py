import json
import re
import os
from typing import Optional, Dict, Any, Tuple, List

import google.genai as genai

from sql_tools import (
    sql_db_list_tables,
    sql_db_schema,
    sql_db_query_checker,
    sql_db_query,
    get_tool_docs_text,
)

tool_docs_text = get_tool_docs_text()

GEMINI_MODEL = os.getenv("GEMINI_MODEL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")



def process_user_request(user_request: str, execute: bool = True, limit: int = 5, max_parts: int = 5) -> Dict[str, Any]:
    if not user_request or not isinstance(user_request, str) or not user_request.strip():
        return {"error": "Empty user request."}

    if not is_complex_request(user_request):
        return nl_to_sql(user_request, execute=execute, limit=limit)

    return handle_complex_request(user_request, execute=execute, limit=limit, max_parts=max_parts)





def _call_gemini(prompt: str) -> str:
    client = genai.Client(api_key=GOOGLE_API_KEY)
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )
    return getattr(resp, "text", str(resp))






def _get_schema_mapping() -> Dict[str, list]:
    tables_resp = sql_db_list_tables()
    tables = tables_resp.get("tables", []) if isinstance(tables_resp, dict) else tables_resp
    if not isinstance(tables, list):
        tables = []
    tables_str = ",".join(tables)
    schema_full = sql_db_schema(tables_str)
    # normalize to table -> [colnames]
    out = {}
    for t, cols in schema_full.items():
        if isinstance(cols, list):
            out[t] = [c["name"] for c in cols]
        else:
            out[t] = []
    return out




def extract_json_from_text(text: str) -> Optional[dict]:
    if not text:
        return None
    start = text.find("{")
    while start != -1:
        stack = []
        for i in range(start, len(text)):
            ch = text[i]
            if ch == "{":
                stack.append("{")
            elif ch == "}":
                if stack:
                    stack.pop()
                if not stack:
                    candidate = text[start:i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)

    m = re.search(r"\{.*?\}", text, re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None




def handle_complex_request(user_request: str, execute: bool = True, limit: int = 5, max_parts: int = 5) -> Dict[str, Any]:

    sub_requests = _split_request_with_llm(user_request, max_parts=max_parts)

    # If splitting didn't actually split (only 1 sub-request and similar to original),
    # treat as simple: run nl_to_sql once and return its result (preserves format).
    if len(sub_requests) == 1:
        single_res = nl_to_sql(sub_requests[0], execute=execute, limit=limit)
        single_res["_sub_request"] = sub_requests[0]
        single_res["_sub_index"] = 1
        single_res["is_complex"] = False
        return single_res

    part_results: List[Dict[str, Any]] = []
    for i, sub in enumerate(sub_requests, start=1):
        res = nl_to_sql(sub, execute=execute, limit=limit)
        res["_sub_request"] = sub
        res["_sub_index"] = i
        part_results.append(res)

    combined_info = _combine_tabular_results(part_results)

    return {
        "original_request": user_request,
        "is_complex": True,
        "sub_requests": sub_requests,
        "part_results": part_results,
        "combined": combined_info,
    }



def _combine_tabular_results(part_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        import pandas as _pd
    except Exception:
        return {"combined": None, "parts": part_results, "combined_possible": False, "reason": "pandas not available"}
    tables = []
    schemas = []
    for pr in part_results:
        rows = pr.get("execution", {}).get("rows")
        if rows and isinstance(rows, list) and len(rows) > 0 and isinstance(rows[0], dict):
            tables.append(rows)
            schemas.append(tuple(sorted(rows[0].keys())) if rows else tuple())
        else:
            return {"combined": None, "parts": part_results, "combined_possible": False}
    if not tables:
        return {"combined": None, "parts": part_results, "combined_possible": False}
    # if column schemas identical 
    if len(set(schemas)) == 1:
        try:
            dfs = [_pd.DataFrame(t) for t in tables]
            combined_df = _pd.concat(dfs, ignore_index=True)
            combined_rows = combined_df.to_dict(orient="records")
            return {"combined": combined_rows, "parts": part_results, "combined_possible": True}
        except Exception as e:
            return {"combined": None, "parts": part_results, "combined_possible": False, "reason": str(e)}
    else:
        return {"combined": None, "parts": part_results, "combined_possible": False}





# Basic static safety checks
def _basic_execute_safety(sql: str) -> Tuple[bool, Optional[str]]:
    if not sql or not isinstance(sql, str):
        return False, "SQL is empty or not a string."
    s = sql.strip()
    first_word = s.split(None, 1)[0].upper() if s else ""
    if first_word not in ("SELECT", "WITH"):
        return False, f"Disallowed first keyword: {first_word}. Only SELECT or WITH allowed."
    upper_sql = s.upper()
    for tok in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "MERGE", "EXEC", "EXECUTE"]:
        if re.search(rf"\b{tok}\b", upper_sql):
            return False, f"Prohibited token detected: {tok}"
    if re.search(r"\bLIMIT\b", upper_sql):
        return False, "LIMIT detected."
    stripped = s.strip()
    if ";" in stripped[:-1]:
        return False, "Multiple statements detected (semicolon in middle)."
    if re.search(r'\bSELECT\b\s*\*', upper_sql):
        return False, "SELECT * detected. Use explicit columns."
    return True, None







def run_checked_query(candidate_sql: str, execute: bool = False, limit: int = 5) -> Dict[str, Any]:
    result: Dict[str, Any] = {"original_sql": candidate_sql}
    checker_out = sql_db_query_checker(candidate_sql)
    result["checker"] = checker_out
   
    validated_sql = candidate_sql
    if isinstance(checker_out, dict) and checker_out.get("fixed_sql"):
        validated_sql = checker_out.get("fixed_sql")
        result["note"] = "Applied checker-proposed fixed_sql."
   

    if not (isinstance(checker_out, dict) and checker_out.get("valid", False)):
        if not checker_out.get("fixed_sql"):
            result["execution"] = {"skipped": True, "reason": "Query invalid according to checker; no fixed_sql provided."}
            return result
        
    if validated_sql != candidate_sql:
        checker_fixed = sql_db_query_checker(validated_sql)
        result["checker_fixed_sql_validation"] = checker_fixed
        if not (isinstance(checker_fixed, dict) and checker_fixed.get("valid", False)):
            result["execution"] = {"skipped": True, "reason": "Checker's fixed_sql failed validation."}
            return result
        validated_sql = checker_fixed.get("fixed_sql") or validated_sql

    safe, reason = _basic_execute_safety(validated_sql)
    if not safe:
        result["execution"] = {"skipped": True, "reason": reason}
        return result
    
    if execute:
        exec_out = sql_db_query(validated_sql, limit=limit)
        result["execution"] = exec_out
    else:
        result["execution"] = {"skipped": True, "reason": "Execution not requested.", "sql_to_execute": validated_sql}
    return result



def is_complex_request(user_request: str, length_threshold: int = 350) -> bool:
    if not user_request or not isinstance(user_request, str):
        return False
    if len(user_request) > length_threshold:
        return True
    low = user_request.lower()
    strong_tokens = [";", " vs ", " compare ", " and also "]
    weak_tokens = [" and ", " each ", " per "]
    strong_hits = sum(1 for t in strong_tokens if t in low)
    weak_hits = sum(1 for t in weak_tokens if t in low)
    return strong_hits >= 1 or weak_hits >= 2




def _finalize_result(generated_sql: str,
                     validated_sql: str,
                     notes: Optional[str],
                     checker: Dict[str, Any],
                     raw_model_responses: List[str],
                     attempts_info: List[Dict[str, Any]],
                     execute: bool,
                     limit: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "generated_sql": generated_sql,
        "validated_sql": validated_sql,
        "notes": notes,
        "checker": checker,
        "attempts": attempts_info,
        "raw_model_responses": raw_model_responses,
    }

    if execute:
        safe, reason = _basic_execute_safety(validated_sql)
        if not safe:
            result["execution"] = {"error": f"Execution blocked: {reason}", "validated_sql": validated_sql}
            return result

        exec_out = sql_db_query(validated_sql, limit=limit)
        result["execution"] = exec_out

    return result






def _split_request_with_llm(user_request: str, max_parts: int = 5) -> List[str]:
    
    schema_mapping = _get_schema_mapping()
    split_prompt = f"""
    You are an advanced SQL task decomposition assistant. Your job is to break a complex natural-language
    request into a small number of simpler, independent sub-requests (maximum {max_parts}).***Strictly You must decide
    intelligently whether splitting is truly necessary or if not then not split it***.

    Your decision rules:

    1. DO split when:
     - The request contains multiple independent goals
       e.g., "calculate revenue AND list top customers"
     - The request mixes cause/problem/effect patterns
       e.g., "find low sales and explain why returns increased"
     - The request asks for comparisons or multi-step analysis
       e.g., "compare A vs B", "for each region AND each month"
     - The request contains separable tasks that require different SQL queries.

    2. **DO NOT split when**:
     - The entire request can be answered using ONE SQL query.
     - The request is long but expresses only ONE intention.
     - Splitting would distort meaning or break logical flow.
     - The request depends on a single dataset/aggregation.
     

    3. If the request is NOT complex:
     → Return a JSON array with ONE element: [ "<original request>" ].

    4. If you DO split:
     - Each sub-request must represent exactly ONE SQL task.
     - Each sub-request must be self-contained and executable independently.
     - Sub-requests must not rely on each other.
     - Avoid mixing multiple goals inside one sub-request.

    FORMAT:
    Return ONLY valid JSON — an array of strings. No explanations.

    EXAMPLES:

    GOOD SPLITS:
    Input:
    "Compute revenue per campaign and compare this to last year's performance."
    Output:
    [
    "Compute revenue per campaign.",
    "Compare campaign revenue to last year's performance."
    ]

    Input:
    "List customers with purchases and also show total orders per region."
    Output:
    [
      "List customers with their purchases.",
     "Show total orders per region."
    ] 

    BAD SPLITS (Never do this):
     Breaking a single-intent request:
    Input: "Get total revenue per campaign."
    Wrong: ["Get total revenue", "Per campaign"]
    Correct: ["Get total revenue per campaign."]

     Mixing unrelated tasks into one:
    Input: "Calculate revenue and list top customers."
    Wrong: ["Calculate revenue and list top customers."]
    Correct: ["Calculate revenue.", "List top customers."]

    SCHEMA (for reference):
    {json.dumps(schema_mapping, indent=2)}

    USER REQUEST:
    {user_request}

    """

    resp_text = _call_gemini(split_prompt)
    parsed = extract_json_from_text(resp_text)
    if isinstance(parsed, list) and parsed:
        parts = [str(p).strip() for p in parsed if isinstance(p, (str, int, float))]
        parts = [p for p in parts if p]
        if not parts:
            return [user_request]
        # If LLM returned only one part and it is effectively the original request, treat as no-split.
        if len(parts) == 1:
            return [user_request]

    # fallback naive split on semicolons or newline or " and also "
    naive = [s.strip() for s in re.split(r'[;\n]', user_request) if s.strip()]
    if len(naive) > 1:
        return naive[:max_parts]
    
    return [user_request]












def nl_to_sql(user_request: str, execute: bool = False, limit: int = 5) -> Dict[str, Any]:
    schema = _get_schema_mapping()
    attempts_info: List[Dict[str, Any]] = []
    raw_model_responses: List[str] = []

    gen_prompt_template = f"""
    You are an expert SQL assistant with deep knowledge of Microsoft SQL Server. Your job is to generate 
    only valid and fully correct T-SQL queries. Always follow official SQL Server syntax and avoid using 
    any syntax or functions from MySQL, PostgreSQL, SQLite, or any other SQL dialect. Use only the exact 
    table names and column names provided in the schema, without guessing or inventing new fields. Your 
    output must be strictly valid T-SQL and should never include unsupported keywords such as LIMIT, USING, 
    ILIKE, or double-quoted identifiers. Every query you produce must be syntactically correct, semantically 
    accurate, safe to execute, and fully compatible with SQL Server

    TOOLS:
    {tool_docs_text}

    Strict rules:
     - Use ONLY SQL Server syntax (T-SQL). Do NOT use LIMIT, backticks, "::type", ILIKE, or other non-T-SQL features.
     - Use GETDATE(), DATEADD(), FORMAT(...,'yyyy-MM'), EOMONTH(), YEAR(), MONTH() where needed.
     - Use TOP instead of LIMIT.
     - Use exact table/column names from the schema below.

    SCHEMA:
    {json.dumps(schema, indent=2)}


    EXAMPLES OF CORRECT T-SQL (You SHOULD imitate these)
    Natural language:
    "List top 10 customers by total spending."

    Correct T-SQL:
    SELECT TOP (10) c.CustomerID, c.Name, SUM(o.TotalAmount) AS TotalSpending
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerID, c.Name
    ORDER BY TotalSpending DESC;


    Natural language:
    "Show monthly sales for 2023."

    Correct T-SQL:
    SELECT FORMAT(OrderDate, 'yyyy-MM') AS YearMonth, SUM(TotalAmount) AS MonthlySales
    FROM Orders
    WHERE YEAR(OrderDate) = 2023
    GROUP BY FORMAT(OrderDate, 'yyyy-MM')
    ORDER BY YearMonth;

    Natural language:
    "Find all employees hired in the last 90 days."

    Correct T-SQL:
    SELECT EmployeeID, FirstName, LastName, HireDate
    FROM Employees
    WHERE HireDate >= DATEADD(DAY, -90, CAST(GETDATE() AS DATE));


    EXAMPLES OF INCORRECT SQL (NEVER DO THIS)

    Using LIMIT:
    SELECT * FROM Orders LIMIT 10;

    Using ILIKE:
    SELECT * FROM Customers WHERE Name ILIKE '%john%';

    Using double quotes for identifiers:
    SELECT "CustomerID" FROM "Orders";

    Using USING join:
    SELECT * FROM Orders JOIN Customers USING (CustomerID);

    Inventing columns:
    SELECT FakeColumn FROM Orders;


    LOGIC RULES YOU MUST APPLY BEFORE GENERATING SQL
    1. Identify the correct table(s) from the schema.
    2. Identify join keys from the schema (never guess).
    3. Use GROUP BY whenever aggregation is used.
    4. Use WHERE for filtering.
    5. Use ORDER BY for ranking.
    6. Use TOP (n) for limiting results.
    7. If necessary information is missing, make a reasonable assumption and note it.


    USER REQUEST:
    {user_request}

    Return ONLY valid JSON (no surrounding text) with keys:
    {{
    "sql": "<SQL query only — NO comments, NO explanations, NO text outside the pure SQL. or NULL>",
    "notes": "<short explanation or assumptions>",
    "parameters": null
    }}
    """

    repair_prompt_template = """
    You previously generated the SQL query shown below, but that query did not pass the validation checks. 
    This means the SQL contained one or more issues such as incorrect or non-existent table names, invalid 
    column references, unsupported or non-T-SQL functions, syntactical mistakes, unsafe keywords, improper 
    JOIN logic, or any other structure that is incompatible with Microsoft SQL Server (T-SQL). Your task now 
    is to thoroughly review the previously generated query, identify all the reasons it failed, and produce a
    corrected version that fully satisfies SQL Server standards. The corrected SQL must use only valid T-SQL
    syntax, adhere strictly to the schema provided to you, maintain read-only behavior, and avoid any 
    MySQL/Postgres/SQLite constructs such as LIMIT, backticks (`table`), ILIKE, USING joins, "::type" casts,
    or date functions not supported by T-SQL. You must ensure all table and column names exactly match the 
    schema, avoid any unsafe operations, and rewrite the logic if necessary to achieve syntactic and semantic 
    correctness. Return only properly structured JSON containing the following keys: 

    INVALID_SQL:
    {invalid_sql}

    VALIDATION_MESSAGE:
    {validation_message}

    USER REQUEST:
    {user_request}

    SCHEMA:
    {schema}


    STRICT RULES (YOU MUST FOLLOW)
    - Use ONLY SQL Server syntax (T-SQL).
    - Use TOP (n), never LIMIT.
    - Do NOT use backticks, double-quoted identifiers, or "::type" casts.
    - Do NOT use ILIKE or USING joins.
    - Do NOT invent table names or columns not in the schema.
    - Always include GROUP BY when required.
    - Maintain read-only behavior (no INSERT/UPDATE/DELETE).
    - Ensure joins use explicit ON conditions.
    - Ensure all table/column names match schema exactly (case-insensitive but spelling must match).

    
    EXAMPLES — How to repair queries

    Example 1 — Incorrect identifier quoting  
     Wrong:
    SELECT "id", "name" FROM Customers LIMIT 10;

    Correct T-SQL:
    SELECT TOP (10) id, name
    FROM Customers;


    Example 2 — Wrong join syntax  
        Wrong:
        SELECT * FROM Orders JOIN Customers USING (CustomerID);

    Correct:
        SELECT o.OrderID, c.CustomerName
        FROM Orders o
        JOIN Customers c ON o.CustomerID = c.CustomerID;


    Example 3 — Invalid or missing GROUP BY  
        Wrong:
        SELECT Region, SUM(Sales), CustomerName FROM Sales;

    Correct:
        SELECT Region, SUM(Sales) AS TotalSales
        FROM Sales
        GROUP BY Region;


    Example 4 — Non-T-SQL functions  
        Wrong:
        SELECT * FROM Orders WHERE OrderDate >= NOW() - INTERVAL '30 days';

    Correct:
        SELECT *
        FROM Orders
        WHERE OrderDate >= DATEADD(DAY, -30, GETDATE());

        

    THINK CAREFULLY BEFORE PRODUCING THE FINAL SQL
    1. Read the validation message and identify ALL errors.
    2. Cross-check table names and columns with the schema.
    3. Fix joins, filters, grouping, and syntax errors.
    4. Rebuild the SQL from scratch if needed.
    5. Ensure the final SQL is syntactically correct T-SQL.
    6. Provide a short explanation in `notes` of what you fixed.


    Your job: produce a corrected Microsoft SQL Server (T-SQL) query that fixes the validation issues above.
    Rules:
     - Use only T-SQL syntax.
     - Use exact table and column names from the provided schema.
     - Do NOT use LIMIT, backticks, double-quoted identifiers, "::type" casting, ILIKE, or other non-T-SQL features.
     - If you cannot fix the SQL, return JSON with "sql": null and explain why in "notes".

    ***Return ONLY valid JSON (no surrounding text) with keys***:
    {{
     "sql": "<SQL query only — NO comments, NO explanations, NO text outside the pure SQL. or Null>",
     "notes": "<short explanation of the changes or failure>",
     "parameters": null
    }}
    """

    candidate_sql = None
    candidate_notes = None
    last_checker: Dict[str, Any] = {}

    for attempt in range(1, 4):
        if attempt == 1:
            prompt = gen_prompt_template
        else:
            invalid_sql_for_prompt = candidate_sql or "<no sql returned>"
            validation_message = last_checker.get("message", "No message") if isinstance(last_checker, dict) else "No message"
            prompt = repair_prompt_template.format(
                invalid_sql=invalid_sql_for_prompt,
                validation_message=validation_message,
                user_request=user_request,
                schema=json.dumps(schema, indent=2),
            )

        raw = _call_gemini(prompt)
        raw_model_responses.append(raw)

        parsed = extract_json_from_text(raw)
        gen_sql = parsed.get("sql") if parsed else None
        gen_notes = parsed.get("notes") if parsed else None

        if gen_sql:
            checker_out = sql_db_query_checker(gen_sql)
        else:
            checker_out = {"valid": False, "message": "No SQL produced", "fixed_sql": None}

        attempts_info.append({
            "attempt": attempt,
            "generated_sql": gen_sql,
            "notes": gen_notes,
            "checker": checker_out,
        })

        if isinstance(checker_out, dict) and checker_out.get("fixed_sql"):
            candidate_sql = checker_out.get("fixed_sql")
            candidate_notes = (gen_notes or "") + " | Applied checker-proposed fix."
            last_checker = sql_db_query_checker(candidate_sql)
            attempts_info[-1]["generated_sql"] = candidate_sql
            attempts_info[-1]["checker"] = last_checker
        else:
            candidate_sql = gen_sql
            candidate_notes = gen_notes
            last_checker = checker_out

        if isinstance(last_checker, dict) and last_checker.get("valid"):
            validated_sql = last_checker.get("fixed_sql") or candidate_sql
            return _finalize_result(
                generated_sql=candidate_sql,
                validated_sql=validated_sql,
                notes=candidate_notes,
                checker=last_checker,
                raw_model_responses=raw_model_responses,
                attempts_info=attempts_info,
                execute=execute,
                limit=limit,
            )

    final_checker = last_checker
    return {
        "Error": "Failed to produce a valid SQL after 3 attempts.",
        "attempts": attempts_info,
        "raw_model_responses": raw_model_responses,
        "last_checker": final_checker
    }