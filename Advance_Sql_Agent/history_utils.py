import json
import os
from datetime import datetime
from typing import List, Dict, Any

HISTORY_FILE = "query_history.json"


def _ensure_history_file() -> None:
    if not os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)


def load_history() -> List[Dict[str, Any]]:
    _ensure_history_file()
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def save_history(history: List[Dict[str, Any]]) -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def add_history_entry(
    history: List[Dict[str, Any]],
    question: str,
    is_complex: bool,
    validated_sql: str | None = None,
    generated_sql: str | None = None,
) -> int:
    entry: Dict[str, Any] = {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "question": question,
        "is_complex": bool(is_complex),
    }
    if validated_sql:
        entry["validated_sql"] = validated_sql
    if generated_sql:
        entry["generated_sql"] = generated_sql

    history.append(entry)
    save_history(history)
    return len(history)


def print_history(history: List[Dict[str, Any]], limit: int = 10) -> None:
    if not history:
        print("\n(No history yet.)")
        return

    limit = max(1, limit)
    total = len(history)
    start = max(0, total - limit)
    subset = list(enumerate(history[start:], start=start + 1))  

    print(f"\n Query History (last {len(subset)} of {total}) ")
    for idx, entry in subset:
        ts = entry.get("timestamp_utc", "?")
        q = entry.get("question", "").replace("\n", " ")
        q_short = (q[:80] + "...") if len(q) > 80 else q
        tag = "complex" if entry.get("is_complex") else "single"
        print(f"[{idx}] ({tag}) {ts}  ::  {q_short}")


def get_history_entry(history: List[Dict[str, Any]], index: int) -> Dict[str, Any] | None:
    if index < 1 or index > len(history):
        return None
    return history[index - 1]
