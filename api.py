from fastapi import FastAPI
from pydantic import BaseModel, EmailStr
import os
from datetime import date

from producer import enqueue_job
from db import get_db
from stock import send_report_email

app = FastAPI()
REPORTS_DIR = "/tmp"

class ReportRequest(BaseModel):
    ticker: str
    email: EmailStr

@app.post("/reports")
def create_report(req: ReportRequest):
    ticker = req.ticker.upper()
    today = date.today().isoformat()
    cached_path = os.path.join(REPORTS_DIR, f"{ticker}_report_{today}.pdf")

    if os.path.exists(cached_path):
        send_report_email(req.email, ticker, cached_path)
        return {"status": "completed", "cached": True}

    payload = {"task_type": "stock_report", "ticker": ticker, "email": req.email}
    stream_id = enqueue_job(payload)
    return {"stream_id": stream_id, "status": "queued", "cached": False}

@app.get("/reports/{stream_id}")
def get_report_status(stream_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error FROM jobs WHERE stream_id = %s",
                (stream_id,)
            )
            row = cur.fetchone()
            if not row:
                return {"status": "not_found"}
            status, error = row
            return {"status": status, "error": error}

@app.get("/health")
def health():
    return {"status": "ok"}