
from pathlib import Path
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import pandas as pd
import json
import io
import traceback
from dotenv import load_dotenv
import os

# load .env for the uvicorn process (must run in project root where .env resides)
load_dotenv()

# reuse your existing SQL agent and history utils
from sql_agent import process_user_request, _call_gemini  # uses your agent pipeline
from history_utils import load_history, add_history_entry, save_history
from connect_db import get_engine  # for fallback raw SQL execution

TEMPLATES_DIR = Path("templates")
if not TEMPLATES_DIR.exists():
    TEMPLATES_DIR.mkdir(parents=True)

app = FastAPI()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# optional static folder
if Path("static").exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

# keep last dataframe in memory for download
LAST_DF = {"df": None, "sql": None, "question": None}

def rows_to_df(rows):
    if rows is None:
        return pd.DataFrame()
    # list of dicts (preferred)
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return pd.DataFrame(rows)
    # list of tuples + separate columns metadata: try to detect
    if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
        try:
            # try to look up columns from last saved validated SQL? skip — fallback to generic
            return pd.DataFrame(rows)
        except Exception:
            return pd.DataFrame(rows)
    try:
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()

def choose_plot(df: pd.DataFrame, chart_type: str = None):
    if df is None or df.empty:
        return None
    from plotly.io import to_html
    import plotly.express as px

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    cat_cols = [c for c in df.columns if c not in numeric_cols]

    try:
        if not chart_type:
            if numeric_cols and cat_cols:
                fig = px.bar(df, x=cat_cols[0], y=numeric_cols[0], title=f"{numeric_cols[0]} by {cat_cols[0]}")
            elif len(numeric_cols) >= 2:
                fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1])
            else:
                return None
        else:
            if chart_type == "bar" and numeric_cols and cat_cols:
                fig = px.bar(df, x=cat_cols[0], y=numeric_cols[0])
            elif chart_type == "line" and numeric_cols and cat_cols:
                fig = px.line(df.sort_values(cat_cols[0]), x=cat_cols[0], y=numeric_cols[0])
            elif chart_type == "scatter" and len(numeric_cols) >= 2:
                fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1])
            elif chart_type == "pie" and cat_cols and numeric_cols:
                fig = px.pie(df, names=cat_cols[0], values=numeric_cols[0])
            else:
                return None
        return to_html(fig, full_html=False, include_plotlyjs="cdn")
    except Exception:
        return None

@app.get("/_envcheck", response_class=PlainTextResponse)
def envcheck():
    """Quick debug route to inspect whether the server sees the env vars."""
    return f"GOOGLE_API_KEY present: {bool(os.getenv('GOOGLE_API_KEY'))}\nGEMINI_MODEL: {os.getenv('GEMINI_MODEL')}\n"

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    history = load_history() or []
    hist_q = [h["question"] for h in history[-10:]] if history else []
    return templates.TemplateResponse("viz_index.html", {
        "request": request,
        "sql": None,
        "table_html": None,
        "plot_div": None,
        "error": None,
        "history": hist_q,
        "question": ""
    })

@app.post("/ask", response_class=HTMLResponse)
async def ask(request: Request, question: str = Form(...), chart: str = Form(None)):
    global LAST_DF
    history = load_history() or []

    try:
        # Use your SQL agent pipeline (NL -> SQL -> validate -> execute)
        out = process_user_request(question, execute=True, limit=200)

        # typical structure: out["execution"]["rows"]
        exec_info = out.get("execution", {}) if isinstance(out, dict) else {}
        rows = None
        if isinstance(exec_info, dict):
            for key in ("rows", "result", "data", "results", "records"):
                if key in exec_info:
                    rows = exec_info.get(key)
                    break
        # sometimes top-level 'rows'
        if rows is None and isinstance(out, dict) and "rows" in out:
            rows = out.get("rows")

        df = rows_to_df(rows or [])
        LAST_DF["df"] = df
        LAST_DF["sql"] = out.get("validated_sql") or out.get("generated_sql")
        LAST_DF["question"] = question

        table_html = df.to_html(classes="table table-sm table-hover", index=False, escape=False)
        plot_div = choose_plot(df, chart_type=chart)

        # add to history with SQL info if available and persist
        add_history_entry(history, question, is_complex=bool(out.get("is_complex", False)),
                          validated_sql=out.get("validated_sql"), generated_sql=out.get("generated_sql"))
        save_history(history)

        hist_q = [h["question"] for h in history[-10:]] if history else []

        # optional summary via agent's _call_gemini (best-effort; ignore errors)
        summary = None
        try:
            if not df.empty:
                head = df.head(10).to_string()
                summary_prompt = f"Write a 2-3 sentence insight summary for this query result:\n{head}"
                summary = _call_gemini(summary_prompt)
        except Exception:
            summary = None

        return templates.TemplateResponse("viz_index.html", {
            "request": request,
            "sql": LAST_DF["sql"],
            "table_html": table_html,
            "plot_div": plot_div,
            "error": None,
            "history": hist_q,
            "question": question,
            "summary": summary
        })

    except Exception as e:
        # primary agent/LLM failed. Attempt a safe fallback:
        tb = traceback.format_exc()
        try:
            hist = load_history() or []
            last_sql = None
            if hist:
                # find last validated_sql or generated_sql
                for h in reversed(hist):
                    last_sql = h.get("validated_sql") or h.get("generated_sql")
                    if last_sql:
                        break
            if last_sql:
                # execute last_sql directly using your DB connection
                engine = get_engine()
                # try to limit rows - if SQL has no TOP/LIMIT you may want to modify; here we execute as-is
                df = pd.read_sql_query(last_sql, engine)
                LAST_DF["df"] = df
                LAST_DF["sql"] = last_sql
                LAST_DF["question"] = question

                table_html = df.to_html(classes="table table-sm table-hover", index=False, escape=False)
                plot_div = choose_plot(df, chart_type=chart)
                hist_q = [h["question"] for h in hist[-10:]] if hist else []

                fallback_notice = f"Agent/LLM failed; showing last saved query result (fallback). Original error: {str(e)}"
                return templates.TemplateResponse("viz_index.html", {
                    "request": request,
                    "sql": LAST_DF["sql"],
                    "table_html": table_html,
                    "plot_div": plot_div,
                    "error": fallback_notice,
                    "history": hist_q,
                    "question": question,
                    "summary": None
                })
        except Exception:
            # if fallback fails, swallow and show original error & traceback
            pass

        return templates.TemplateResponse("viz_index.html", {
            "request": request,
            "sql": None,
            "table_html": None,
            "plot_div": None,
            "error": f"{str(e)}\n\nTraceback:\n{tb}",
            "history": [h["question"] for h in (history or [])[-10:]] if history else [],
            "question": question,
            "summary": None
        })

@app.get("/download_csv")
async def download_csv():
    df = LAST_DF.get("df")
    if df is None or df.empty:
        return {"error": "No data available to download."}
    buf = io.BytesIO()
    # write CSV bytes
    buf.write(df.to_csv(index=False).encode("utf-8"))
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=result.csv"})

@app.get("/repeat/{idx}")
async def repeat(idx: int):
    history = load_history() or []
    if idx < 1 or idx > len(history):
        return RedirectResponse("/", status_code=302)
    q = history[idx - 1]["question"]
    return RedirectResponse(url=f"/run_and_show?q={q}", status_code=302)

@app.get("/run_and_show", response_class=HTMLResponse)
async def run_and_show(request: Request, q: str = None):
    if not q:
        return RedirectResponse("/", status_code=302)
    return await ask(request, question=q)


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    raw_history = load_history() or []

    normalized_history = []
    for h in raw_history:
        if isinstance(h, dict):
            normalized_history.append({
                "question": h.get("question", ""),
                "validated_sql": h.get("validated_sql"),
                "generated_sql": h.get("generated_sql")
            })
        else:
            # fallback for corrupted / old entries
            normalized_history.append({
                "question": str(h),
                "validated_sql": None,
                "generated_sql": None
            })

    return templates.TemplateResponse(
        "history.html",
        {
            "request": request,
            "history": normalized_history[::-1]  # latest first
        }
    )

@app.get("/customer-analysis", response_class=HTMLResponse)
async def customer_analysis(request: Request):
    return templates.TemplateResponse(
        "feature_page.html",
        {
            "request": request,
            "title": "Customer Analysis",
            "description": "Analyze top customers and behavior patterns",
            "default_query": "Show top 10 customers by total revenue"
        }
    )


@app.get("/revenue-insights", response_class=HTMLResponse)
async def revenue_insights(request: Request):
    return templates.TemplateResponse(
        "feature_page.html",
        {
            "request": request,
            "title": "Revenue Insights",
            "description": "Track revenue trends and growth",
            "default_query": "Show monthly revenue trends for the last year"
        }
    )


@app.get("/growth-metrics", response_class=HTMLResponse)
async def growth_metrics(request: Request):
    return templates.TemplateResponse(
        "feature_page.html",
        {
            "request": request,
            "title": "Growth Metrics",
            "description": "Monitor growth and performance indicators",
            "default_query": "Show growth metrics month over month"
        }
    )


@app.get("/user-demographics", response_class=HTMLResponse)
async def user_demographics(request: Request):
    return templates.TemplateResponse(
        "feature_page.html",
        {
            "request": request,
            "title": "User Demographics",
            "description": "Explore customer segments and profiles",
            "default_query": "Show customer distribution by region"
        }
    )
