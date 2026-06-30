from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
import os
import json
from datetime import date

from producer import enqueue_job
from db import get_db
from stock import send_report_email

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for now (dev + testing)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
REPORTS_DIR = "/tmp"


class ReportRequest(BaseModel):
    ticker: str
    email: EmailStr


def _paths_for(ticker: str, day: str | None = None):
    """Build the cached PDF/JSON paths for a ticker on a given day (today by default)."""
    day = day or date.today().isoformat()
    base = os.path.join(REPORTS_DIR, f"{ticker}_report_{day}")
    return f"{base}.pdf", f"{base}.json"


def _load_report_data(json_path: str):
    """Load the cached indicator/history payload written by the worker, if present."""
    if not os.path.exists(json_path):
        return None
    with open(json_path) as f:
        return json.load(f)


@app.post("/reports")
def create_report(req: ReportRequest):
    ticker = req.ticker.upper()
    pdf_path, json_path = _paths_for(ticker)

    if os.path.exists(pdf_path):
        send_report_email(req.email, ticker, pdf_path)
        return {
            "status": "completed",
            "cached": True,
            "data": _load_report_data(json_path),
        }

    payload = {"task_type": "stock_report", "ticker": ticker, "email": req.email}
    stream_id = enqueue_job(payload)
    return {"stream_id": stream_id, "status": "queued", "cached": False}


@app.get("/reports/{stream_id}")
def get_report_status(stream_id: str):
    """
    Poll job status. Once the worker has completed the job, the response
    includes the ticker and date so the frontend can fetch the full
    indicator/history payload from /reports/{ticker}/data.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error, ticker, created_at::date FROM jobs WHERE stream_id = %s",
                (stream_id,)
            )
            row = cur.fetchone()
            if not row:
                return {"status": "not_found"}
            status, error, ticker, created_date = row
            response = {"status": status, "error": error}
            if status == "completed" and ticker:
                response["ticker"] = ticker
                response["data_url"] = f"/reports/{ticker}/data?day={created_date.isoformat()}"
            return response


@app.get("/reports/{ticker}/data")
def get_report_data(ticker: str, day: str | None = None):
    """
    Return the full set of computed indicators (sectioned latest values,
    a flat lookup, and a recent history time series for charting) for a
    ticker's report. This is the same payload the worker embeds in the PDF
    summary table, so the frontend stays in sync with the report.

    Query params:
        day (str, optional): YYYY-MM-DD report date; defaults to today.
    """
    ticker = ticker.upper()
    _pdf_path, json_path = _paths_for(ticker, day)
    data = _load_report_data(json_path)
    if data is None:
        raise HTTPException(status_code=404, detail="Report data not found for this ticker/date.")
    return data


@app.get("/health")
def health():
    return {"status": "ok"}