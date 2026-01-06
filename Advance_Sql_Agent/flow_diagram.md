# ============================================
#        SQL AGENT – PIPELINE FLOW DIAGRAM
#        High-Level Architecture (ASCII)
# ============================================


                 ┌───────────────────────────────┐
                 │          USER QUERY           │
                 │                               │
                 │                               │
                 └───────────────────────────────┘
                                │
                                ▼
                 ┌───────────────────────────────┐
                 │      PROCESS_USER_REQUEST     │
                 │     (Entry Point Function)    │
                 └───────────────────────────────┘
                                │
                     ┌──────────┴──────────┐
                     ▼                     ▼
         ┌────────────────────┐   ┌─────────────────────┐
         │ SIMPLE REQUEST?    │   │ COMPLEX REQUEST?    │
         │ is_complex_request │   │    (True)           │
         └────────────────────┘   └─────────────────────┘
                     │                     │
          (False)    │                     │   (True)
                     ▼                     ├─────────────────────────────┐
                                                                         │
# ============================================                           │
#        SIMPLE REQUEST PROCESSING                                       │
# ============================================                           │
       ▼                                                                 │
       ┌────────────────────────────────────────────┐                    │
       │                 nl_to_sql()                │                    │
       │    Converts NLP → SQL (single query)       │                    │
       └────────────────────────────────────────────┘                    │
                       │                                                 │
                       ▼                                                 │
        ┌───────────────────────────────────────────┐                    │
        │        PHASE 1: SCHEMA LOADING            │                    │
        │   _get_schema_mapping()                   │                    │
        │   └── sql_db_list_tables()                │                    │
        │   └── sql_db_schema()                     │                    │
        └───────────────────────────────────────────┘                    │
                       │                                                 │
                       ▼                                                 │
        ┌───────────────────────────────────────────┐                    │
        │     PHASE 2: QUERY GENERATION (LLM)        │                   │
        │         _call_gemini(prompt)               │                   │
        │     extract_json_from_text(raw_output)     │                   │
        └───────────────────────────────────────────┘                    │
                       │                                                 │
                       ▼                                                 │
        ┌───────────────────────────────────────────┐                    │
        │      PHASE 3: SQL VALIDATION CHECKER       │                   │
        │        sql_db_query_checker()              │                   │
        │     (may return fixed_sql)                 │                   │
        └───────────────────────────────────────────┘                    │
                       │                                                 │
                       ▼                                                 │
        ┌───────────────────────────────────────────┐                    │
        │       PHASE 4: SAFETY FILTERS              │                   │
        │      _basic_execute_safety(sql)            │                   │
        │  Blocks: INSERT/DELETE/UPDATE/LIMIT/* etc  │                   │
        └───────────────────────────────────────────┘                    │
                       │                                                 │
                       ▼                                                 │
        ┌───────────────────────────────────────────┐                    │
        │       PHASE 5: SQL EXECUTION (optional)    │                   │
        │         sql_db_query(sql, limit)           │                   │
        │         limit enforced by caller           │                   │
        └───────────────────────────────────────────┘                    │
                       │                                                 │
                       ▼                                                 │
        ┌───────────────────────────────────────────┐                    │
        │            FINAL RESPONSE JSON             │                   │
        │   { generated_sql, validated_sql, rows }   │                   │
        └───────────────────────────────────────────┘                    │
                                                                         │
                                                                         │
# ============================================                           │
#          COMPLEX REQUEST PROCESSING                                    │
# ============================================   ________________________│
                                                |
                                                ▼
             ┌────────────────────────────────────┐
             │      handle_complex_request()       │
             └────────────────────────────────────┘
                              │
                              ▼
             ┌────────────────────────────────────┐
             │   PHASE 1: SUB-REQUEST SPLITTER    │
             │      _split_request_with_llm()     │
             │   Returns: [task1, task2, ...]     │
             └────────────────────────────────────┘
                              │
                              ▼
             ┌────────────────────────────────────┐
             │   PHASE 2: PARALLEL SUB-QUERIES    │
             │   For each sub-request:            │
             │      └── nl_to_sql(sub_request)    │
             └────────────────────────────────────┘
                              │
                              ▼
             ┌────────────────────────────────────┐
             │   PHASE 3: RESULT COMBINATION      │
             │     _combine_tabular_results()     │
             │   Uses pandas (if schemas match)   │
             └────────────────────────────────────┘
                              │
                              ▼
             ┌────────────────────────────────────┐
             │        FINAL COMPLEX OUTPUT        │
             │ { sub_requests, part_results,      │
             │   combined_rows?, combined_state } │
             └────────────────────────────────────┘


# ============================================
#               SHARED UTILITIES
# ============================================

    ┌──────────────────────────────────────────────┐
    │ extract_json_from_text()                     │
    │ sql_db_query_checker()                       │
    │ sql_db_query()                               │
    │ _basic_execute_safety()                      │
    │ _get_schema_mapping()                        │
    │ _call_gemini()                               │
    └──────────────────────────────────────────────┘

# ============================================
#                END OF PIPELINE
# ============================================
