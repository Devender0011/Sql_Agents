
# # main.py
# import json
# import pandas as pd
# from datetime import date, datetime

# from sql_agent import nl_to_sql


# def normalize_row_values(row):
#     out = {}
#     for k, v in row.items():
#         if isinstance(v, (date, datetime)):
#             out[k] = v.isoformat()
#         else:
#             out[k] = v
#     return out


# def pretty_print_execution(result):
#     # Handle errors before anything else
#     if "error" in result and not result.get("execution"):
#         print("ERROR:")
#         print(result.get("error"))

#         if result.get("raw_model_response"):
#             print("Raw LLM Response")
#             print(result["raw_model_response"])

#         return

#     # Print raw model output for debugging
#     if "raw_model_response" in result:
#         print("\n=== Raw LLM Response ===")
#         print(result["raw_model_response"])

#     # Show generated & validated SQL
#     print("\n=== SQL INFO ===")
#     print("Generated SQL:", result.get("generated_sql"))
#     print("Validated SQL:", result.get("validated_sql"))

#     # Normalize rows
#     rows = result.get("execution", {}).get("rows", [])
#     norm_rows = [normalize_row_values(r) for r in rows]

#     # Pretty JSON
#     pretty_json = {
#         "generated_sql": result.get("generated_sql"),
#         "validated_sql": result.get("validated_sql"),
#         "notes": result.get("notes"),
#         "execution": {
#             "rows": norm_rows,
#             "sql_executed": result.get("execution", {}).get("sql_executed")
#         }
#     }

#     print("\n=== Pretty JSON ===")
#     print(json.dumps(pretty_json, indent=2))

#     # Pretty table
#     print("\n=== Table Output ===")
#     if norm_rows:
#         df = pd.DataFrame(norm_rows)
#         print(df.to_string(index=False))   # no tabulate dependency
#     else:
#         print("(no rows returned)")


# if __name__ == "__main__":
#     print("SQL Agent Ready. Ask natural-language questions about your database.")
#     print("Type 'exit' to quit.\n")

#     while True:
#         q = input("\nAsk your question: ")
#         if q.lower() in ("exit", "quit"):
#             print("\nGoodbye!")
#             break
#         print("\nProcessing your query...")
#         # Run NL → SQL → Execution
#         out = nl_to_sql(q, execute=True, limit=5)
#         # Print results nicely
#         pretty_print_execution(out)















# main.py
import json
import pandas as pd
from datetime import date, datetime

# use the new top-level entry (decides whether to split)
from sql_agent import process_user_request


def normalize_row_values(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _print_table_rows(rows, title="Table"):
    print(f"\n=== {title} ===")
    if not rows:
        print("(no rows returned)")
        return
    norm_rows = [normalize_row_values(r) for r in rows]
    try:
        df = pd.DataFrame(norm_rows)
        print(df.to_string(index=False))
    except Exception:
        # fallback to pretty JSON if pandas fails
        print(json.dumps(norm_rows, indent=2))


def pretty_print_execution(result):
    # Top-level error (no execution)
    if "error" in result and not result.get("execution"):
        print("ERROR:")
        print(result.get("error"))

        if result.get("raw_model_responses"):
            print("\n=== Raw LLM Responses ===")
            for i, r in enumerate(result["raw_model_responses"], start=1):
                print(f"\n--- Response #{i} ---")
                print(r)

        return

    # If this is a complex request (split into parts)
    if result.get("is_complex"):
        print("\n=== COMPLEX REQUEST ===")
        print("Original request:")
        print(result.get("original_request"))

        sub_requests = result.get("sub_requests", [])
        part_results = result.get("part_results", [])

        # Print per-part summaries
        for idx, pr in enumerate(part_results, start=1):
            print(f"\n--- Part #{idx} ---")
            sub = pr.get("_sub_request", sub_requests[idx-1] if idx-1 < len(sub_requests) else "(unknown)")
            print("Sub-request:", sub)
            if pr.get("error"):
                print("Error:", pr["error"])
                continue

            print("Generated SQL:", pr.get("generated_sql"))
            print("Validated SQL:", pr.get("validated_sql"))
            notes = pr.get("notes")
            if notes:
                print("Notes:", notes)

            exec_info = pr.get("execution", {})
            if exec_info.get("rows"):
                _print_table_rows(exec_info["rows"], title=f"Part #{idx} Results")
            else:
                # print exec_info for diagnostics
                print("Execution info:", json.dumps(exec_info, indent=2))

            # show raw model responses for this part if present
            raw_responses = pr.get("raw_model_responses")
            if raw_responses:
                print("\nRaw LLM Responses for this part:")
                for i, r in enumerate(raw_responses, start=1):
                    print(f"\n--- Part {idx} Response #{i} ---")
                    print(r)

        # Try to print combined if available
        combined = result.get("combined", {})
        if combined:
            if combined.get("combined_possible"):
                print("\n=== COMBINED RESULT (concatenated parts) ===")
                _print_table_rows(combined.get("combined"), title="Combined Results")
            else:
                reason = combined.get("reason")
                print("\nNo combined result available.", ("Reason: " + reason) if reason else "")
        return

    # Non-complex (single) result path
    # Print raw LLM responses (if any)
    if "raw_model_responses" in result:
        print("\n=== Raw LLM Responses ===")
        for i, r in enumerate(result["raw_model_responses"], start=1):
            print(f"\n--- Response #{i} ---")
            print(r)

    # Show generated & validated SQL
    print("\n=== SQL INFO ===")
    print("Generated SQL:", result.get("generated_sql"))
    print("Validated SQL:", result.get("validated_sql"))
    notes = result.get("notes")
    if notes:
        print("Notes:", notes)

    # Execution output
    exec_info = result.get("execution", {})
    rows = exec_info.get("rows", [])
    if rows:
        _print_table_rows(rows, title="Query Results")
    else:
        # show execution diagnostics
        print("\n=== Execution Info ===")
        print(json.dumps(exec_info, indent=2))


if __name__ == "__main__":
    print("SQL Agent Ready. Ask natural-language questions about your database.")
    print("Type 'exit' to quit.\n")

    while True:
        q = input("\nAsk your question: ")
        if q.lower() in ("exit", "quit"):
            print("\nGoodbye!")
            break
        print("\nProcessing your query...")
        # Use process_user_request (it will decide whether to split)
        out = process_user_request(q, execute=True, limit=5)
        pretty_print_execution(out)



















