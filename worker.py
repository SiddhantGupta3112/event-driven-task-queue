import logging
from producer import redis_client
from db import get_db
import random
import time
from typing import cast
from stock import generate_stock_report

def worker_group_exists():
    logging.info("Worker setup: Checking if consumer group exists...")

    try:
        groups = redis_client.xinfo_groups("jobs")
        if any(g["name"] == "workers" for g in groups):
            logging.info("Worker setup: Group 'workers' exists")
            return True
    except Exception:
        pass

    try:
        logging.info("Worker setup: Creating 'workers' consumer group...")
        redis_client.xgroup_create(name="jobs", groupname="workers", id="$", mkstream=True)
        logging.info("Worker setup: Consumer group 'workers' created")
        return True
    except Exception as e:
        logging.error(f"Worker setup: Error creating consumer group. Error: {e}")
        return False


def consume_jobs(
    worker_id: str,
    max_iterations: int | None = None,
    max_idle_polls: int | None = None
):
    logging.info(f"{worker_id}: Worker starting up...")
    iterations = 0
    polls = 0
    while True:
        if max_iterations is not None and iterations >= max_iterations:
            logging.info(f"{worker_id}: Max iterations reached. Exiting.")
            return
        
        
        logging.info(f"{worker_id}: Looking for jobs...")
        messages = redis_client.xreadgroup(
                groupname="workers",
                consumername=worker_id,
                count=1,
                block=2000,
                streams={"jobs": ">"}
        )
        
        if not messages:
            logging.info(
                f"{worker_id}: No job found. Idle poll {polls + 1}."
            )
            polls += 1
            if max_idle_polls is not None and polls >= max_idle_polls:
                logging.info(f"{worker_id}: Idling. Exiting.")
                return
        
            continue
        
        stream, entries = cast(list, messages)[0]
        stream_id, payload = entries[0]
        polls = 0
        iterations += 1
        
        logging.info(f"{worker_id}: Got job {stream_id} with payload {payload}")
        
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    
                        logging.info(f"{worker_id}: Checking job status in Postgres...")
                        cur.execute(
                            '''
                            UPDATE jobs
                            SET status = 'processing',
                                worker_id = %s,
                                started_at = NOW(),
                                attempts = attempts + 1
                            WHERE stream_id = %s
                            AND status = 'pending';
                            ''',
                            (worker_id, stream_id)
                        )

                        if cur.rowcount == 0:
                            logging.info(
                                f"{worker_id}: Job {stream_id} already processed. "
                                f"Acknowledging Redis message."
                            )
                            conn.commit()
                            redis_client.xack("jobs", "workers", stream_id)
                            continue
                            
                        conn.commit()
        except Exception as e:
            logging.exception(
                f"{worker_id}: Failed to mark "
                f"job {stream_id} as processing. Error: {e}"
            )
            continue
        
        if payload.get("task_type") == "stock_report":
            result = generate_stock_report(
                ticker=payload["ticker"],
                email=payload["email"]
                )
            status = result["status"]
            error_msg = result.get("error")
        else:
            time.sleep(random.uniform(0.5, 2))
            status = random.choice(["failed", "completed"])
            error_msg = "Job failed due to simulated error" if status == "failed" else None

        try:
            with get_db() as conn:
                with conn.cursor() as cur:

                    logging.info(
                        f"{worker_id}: Updating final status to '{status}' "
                        f"for job {stream_id}"
                    )

                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = %s,
                            completed_at = NOW(),
                            error = %s
                        WHERE stream_id = %s;
                        """,
                        (status, error_msg, stream_id)
                    )

                    conn.commit()

                    logging.info(
                        f"{worker_id}: Job {stream_id} "
                        f"marked as {status}."
                    )

        except Exception as e:

            logging.exception(
                f"{worker_id}: Failed to update final "
                f"status for job {stream_id}. Error: {e}"
            )

            continue
        try:
            redis_client.xack(
                "jobs",
                "workers",
                stream_id
            )

            logging.info(
                f"{worker_id}: Job {stream_id} "
                f"acknowledged in Redis."
            )

        except Exception as e:

            logging.exception(
                f"{worker_id}: Failed to ACK "
                f"job {stream_id}. Error: {e}"
            )


