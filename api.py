from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

import json
import os
from datetime import date

from producer import enqueue_job
from db import get_db
from stock import send_report_email

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REPORTS_DIR = "/tmp"


class ReportRequest(BaseModel):
    ticker: str
    email: EmailStr


def report_paths(ticker: str, report_date: str | None = None):
    report_date = report_date or date.today().isoformat()

    base = os.path.join(
        REPORTS_DIR,
        f"{ticker.upper()}_report_{report_date}"
    )

    return {
        "pdf": f"{base}.pdf",
        "json": f"{base}.json",
    }


def load_report_data(path: str):
    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        return json.load(f)


@app.post("/reports")
def create_report(req: ReportRequest):

    ticker = req.ticker.upper()

    paths = report_paths(ticker)

    # Cached report
    if os.path.exists(paths["pdf"]):

        send_report_email(
            req.email,
            ticker,
            paths["pdf"]
        )

        return {
            "status": "completed",
            "cached": True,
            "data": load_report_data(paths["json"])
        }

    payload = {
        "task_type": "stock_report",
        "ticker": ticker,
        "email": req.email,
    }

    stream_id = enqueue_job(payload)

    return {
        "status": "queued",
        "cached": False,
        "stream_id": stream_id,
    }


@app.get("/reports/{stream_id}")
def get_report_status(stream_id: str):

    with get_db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    status,
                    error,
                    payload->>'ticker' AS ticker,
                    created_at::date
                FROM jobs
                WHERE stream_id = %s
                """,
                (stream_id,)
            )

            row = cur.fetchone()

            if row is None:
                return {
                    "status": "not_found"
                }

            status, error, ticker, created_date = row

            response = {
                "status": status,
                "error": error,
            }

            if status == "completed":

                response["ticker"] = ticker

                response["data_url"] = (
                    f"/reports/{ticker}/data"
                    f"?day={created_date.isoformat()}"
                )

            return response


@app.get("/reports/{ticker}/data")
def get_report_data(
    ticker: str,
    day: str | None = None
):

    ticker = ticker.upper()

    paths = report_paths(ticker, day)

    report = load_report_data(paths["json"])

    if report is None:
        raise HTTPException(
            status_code=404,
            detail="Report not found."
        )

    return report


@app.get("/health")
def health():
    return {
        "status": "ok"
    }