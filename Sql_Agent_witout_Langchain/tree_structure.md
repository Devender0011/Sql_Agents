PROCESS_USER_REQUEST(user_request)
└─> Validate input
   ├─ if empty -> RETURN {"error":"Empty user request."}
   └─> is_complex_request(user_request)
       ├─ Decision: SIMPLE (False)
       │   └─> nl_to_sql(user_request)                          # SIMPLE PATH
       │       ├─ _get_schema_mapping()                         -> schema_dict
       │       │    └─ sql_db_list_tables() + sql_db_schema()
       │       ├─ build generation prompt with schema & rules
       │       ├─ _call_gemini(prompt)                          -> raw_llm_response
       │       ├─ extract_json_from_text(raw_llm_response)      -> parsed_json
       │       │    └─ parsed_json["sql"], parsed_json["notes"]
       │       ├─ if no SQL -> RETURN { "sql": null, "notes": ... }
       │       ├─ sql_db_query_checker(gen_sql)                 -> checker_out
       │       │    ├─ if checker_out.fixed_sql -> candidate_sql = fixed_sql
       │       │    └─ else candidate_sql = gen_sql
       │       ├─ (repair loop up to 3 attempts)                -> may re-call LLM with repair_prompt
       │       ├─ _basic_execute_safety(validated_sql)          -> (safe?, reason)
       │       │    ├─ if NOT safe -> RETURN execution blocked (reason)
       │       │    └─ if safe -> sql_db_query(validated_sql, limit)
       │       └─ RETURN finalized result:
       │             {
       │               generated_sql, validated_sql, notes,
       │               checker, attempts, raw_model_responses,
       │               execution: { rows/metadata } or error
       │             }
       └─ Decision: COMPLEX (True)
           └─> handle_complex_request(user_request)
               ├─ _split_request_with_llm(user_request, max_parts)
               │    └─ returns list[sub_request_1, sub_request_2, ...]
               ├─ if returned list length == 1
               │    └─ fallback -> call nl_to_sql(original) (treat as simple)
               ├─ else (multiple sub-requests)
               │    └─ for each sub_request_i:
               │         ├─ call nl_to_sql(sub_request_i)   # executes full SIMPLE PATH for each part
               │         └─ collect part_result_i (SQL, checker, execution)
               ├─ after loop -> _combine_tabular_results(part_results)
               │    ├─ tries to combine parts using pandas:
               │    │    - requires each part to return rows (list of dicts)
               │    │    - requires identical column names (order-insensitive)
               │    │    - if OK -> concat and return combined_rows
               │    └─ if combine impossible -> return part_results + reason
               └─ RETURN:
                   {
                     original_request, is_complex: True,
                     sub_requests, part_results, combined: { combined_possible, combined_rows/ reason }
                   }

SUPPORTING/SHARED NODES
└─ sql_db_query_checker(sql)
   └─ validates SQL, may return { valid: bool, fixed_sql: str, message }
└─ _basic_execute_safety(sql)
   └─ enforces SELECT/WITH only, blocks destructive tokens, avoids SELECT *, LIMIT, multi-statement
└─ sql_db_query(sql, limit)
   └─ executes validated SQL, returns rows + metadata or DB error
└─ _call_gemini(prompt)
   └─ calls LLM; returns raw response string (then parsed)
└─ extract_json_from_text(raw_text)
   └─ extracts first JSON object from text (LLM output -> JSON)
└─ _get_schema_mapping()
   └─ calls sql_db_list_tables() and sql_db_schema(), returns {table: [cols]}














Function → responsibilities (ordered flow)

Use this to trace exactly which function is called when and what it returns.

process_user_request(user_request, execute=True, limit=5, max_parts=5)

Input: user_request (string)

Calls: is_complex_request

Branches: nl_to_sql (simple) or handle_complex_request (complex)

Returns final JSON to user.

is_complex_request(user_request, length_threshold=350)

Input: user_request

Logic: length > threshold OR presence of strong tokens (;, and/or, compare, and also) OR many weak tokens

Output: True/False decision

nl_to_sql(user_request, execute=False, limit=5) — SIMPLE path

Steps:
a. schema = _get_schema_mapping()

Calls: sql_db_list_tables(), sql_db_schema()

Returns dict {table: [colnames]}.
b. Build gen_prompt_template string (includes schema & rules).
c. Loop up to 3 attempts:

raw = _call_gemini(prompt) → raw LLM string

parsed = extract_json_from_text(raw) → parsed JSON with keys sql, notes

gen_sql = parsed.get("sql")

If gen_sql:

checker_out = sql_db_query_checker(gen_sql) → may propose fixed_sql

If checker_out.fixed_sql: adopt that & re-validate

If checker_out.valid: validated_sql = checker_out.fixed_sql or gen_sql → go to safety

Else continue repair prompt
d. _basic_execute_safety(validated_sql):

If not safe → return execution blocked
e. If safe and execute=True → sql_db_query(validated_sql, limit)
f. Return structure produced by _finalize_result.

_get_schema_mapping()

Calls sql_db_list_tables() → list of tables

Calls sql_db_schema(tables_str) → full schema

Normalizes to mapping {table: [colnames]}

_call_gemini(prompt)

Calls genai client with generation_config (e.g. temperature)

Returns raw model response string (LLM output)

extract_json_from_text(raw_text)

Parses the first JSON object found in raw LLM text

Returns parsed dict or None

sql_db_query_checker(candidate_sql)

External checker that validates SQL & may return fixed_sql

Return example: { valid: True/False, fixed_sql: "...", message: "..." }

_basic_execute_safety(validated_sql)

Enforces safe tokens (only SELECT/WITH, no destructive tokens)

Disallows SELECT *, LIMIT, multi-statement semicolons

Returns (True, None) or (False, reason)

sql_db_query(validated_sql, limit)

Executes on DB; returns {"rows": [...], "error": ...}

handle_complex_request(user_request, execute=True, limit=5, max_parts=5)

Calls _split_request_with_llm(user_request, max_parts) to split into sub-requests

If only 1 part returned → fallback to nl_to_sql(original) (not splitting)

For each sub-request -> call nl_to_sql(sub_request, execute=execute, limit=limit) and collect part_result

After all parts -> _combine_tabular_results(part_results)

Uses pandas to concat if identical schemas

Else returns parts + reason combined not possible

Return aggregated structure with sub_requests, part_results, combined info

_split_request_with_llm(user_request, max_parts=5)

Builds split prompt including schema mapping

Calls _call_gemini(split_prompt)

extract_json_from_text → expects array of strings (sub-requests)

Fallback: naive split on ; / newline

_combine_tabular_results(part_results)

Checks each part_result has execution.rows (list of dicts)

Checks column schemas identical (set equality)

If matches: uses pandas to concat and return combined rows

Else returns combined_possible: False with parts

_finalize_result(...)

Builds final return JSON including SQLs, checker, attempts, raw_model_responses

If execute requested and safety ok -> attaches execution with sql_db_query output

If blocked -> attaches execution with blocked reason