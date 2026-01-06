import json
import re
from datetime import date, datetime
import pandas as pd
from sql_agent import process_user_request
from history_utils import load_history, add_history_entry, print_history, get_history_entry


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
            sub = pr.get("_sub_request", sub_requests[idx - 1] if idx - 1 < len(sub_requests) else "(unknown)")
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


def _handle_history_command(cmd: str, history: list) -> bool:
    """
    Handle commands related to history.
    Returns True if the command was handled and main loop should continue.
    """
    stripped = cmd.strip().lower()

    # history or history N
    if stripped.startswith("history"):
        parts = stripped.split()
        limit = 10
        if len(parts) == 2 and parts[1].isdigit():
            limit = int(parts[1])
        print_history(history, limit=limit)
        return True

    # repeat N  (rerun Nth question through normal pipeline)
    m = re.match(r"^repeat\s+(\d+)$", stripped)
    if m:
        idx = int(m.group(1))
        entry = get_history_entry(history, idx)
        if not entry:
            print(f"\nNo history entry at index {idx}.")
            return True
        q = entry.get("question", "")
        print(f"\nRe-running history item [{idx}]:")
        print(q)
        print("\nProcessing your query again...")
        out = process_user_request(q, execute=True, limit=5)
        pretty_print_execution(out)
        # We do NOT automatically re-add a new history entry for repeat;
        # if you want to, you can append here as well.
        return True

    return False


if __name__ == "__main__":
    print("SQL Agent Ready. Ask natural-language questions about your database.")
    print("Type 'exit' to quit.")
    print("Extra commands:")
    print("  history           → show last 10 queries")
    print("  history N         → show last N queries")
    print("  repeat N          → rerun Nth query from history\n")

    # load existing history at startup
    history = load_history()

    while True:
        q = input("\nEnter your natural-language query. Press ENTER when done.\nQuery: ")
        if q.strip().lower() in ("exit", "quit"):
            print("\nGoodbye!")
            break

        # Check if this is a history-related command
        if _handle_history_command(q, history):
            continue

        print("\nProcessing... (this will request execution if you passed --execute)")
        out = process_user_request(q, execute=True, limit=5)
        pretty_print_execution(out)

        # add to history
        is_complex = bool(out.get("is_complex", False))
        validated_sql = None
        generated_sql = None

        if not is_complex:
            validated_sql = out.get("validated_sql")
            generated_sql = out.get("generated_sql")

        idx = add_history_entry(
            history=history,
            question=q,
            is_complex=is_complex,
            validated_sql=validated_sql,
            generated_sql=generated_sql,
        )
        print(f"\n(Saved to history as entry #{idx})")
