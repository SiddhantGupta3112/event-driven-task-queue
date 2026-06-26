from producer import redis_client
from db import get_db
import logging
import time
import random


def cleanup_jobs(
    reaper_id: str,
    idle_ms: int = 30000,
    count: int = 10
):
    logging.info(
        f"{reaper_id}: Reaper starting. "
        f"Checking up to {count} pending jobs."
    )

    try:
        pending = redis_client.xpending_range(
            "jobs",
            "workers",
            min="-",
            max="+",
            count=count
        )
    except Exception as e:
        logging.exception(
            f"{reaper_id}: Failed to query Redis PEL. Error: {e}"
        )
        return

    if not pending:
        logging.info(
            f"{reaper_id}: No pending jobs found. Shutting down."
        )
        return

    logging.info(
        f"{reaper_id}: Found {len(pending)} pending jobs."
    )

    for job in pending:

        stream_id = job["message_id"]

        logging.info(
            f"{reaper_id}: Attempting recovery of "
            f"job {stream_id}."
        )

        try:
            claimed = redis_client.xclaim(
                "jobs",
                "workers",
                reaper_id,
                idle_ms,
                [stream_id]
            )

        except Exception as e:
            logging.exception(
                f"{reaper_id}: Failed to claim "
                f"job {stream_id}. Error: {e}"
            )
            continue

        if not claimed:
            logging.warning(
                f"{reaper_id}: Redis returned no claimed "
                f"message for {stream_id}. "
                f"It may not have exceeded idle time."
            )
            continue

        stream_id, payload = claimed[0]

        logging.info(
            f"{reaper_id}: Successfully claimed "
            f"job {stream_id}."
        )

        # --------------------------------------------------
        # Mark processing
        # --------------------------------------------------

        try:
            with get_db() as conn:
                with conn.cursor() as cur:

                    logging.info(
                        f"{reaper_id}: Marking "
                        f"job {stream_id} as processing."
                    )

                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'processing',
                            worker_id = %s,
                            started_at = NOW(),
                            attempts = attempts + 1
                        WHERE stream_id = %s
                        AND status IN ('pending', 'processing');
                        """,
                        (reaper_id, stream_id)
                    )

                    if cur.rowcount == 0:

                        logging.info(
                            f"{reaper_id}: Job {stream_id} "
                            f"already completed. "
                            f"Acknowledging Redis entry."
                        )
                        conn.commit()
                        redis_client.xack(
                            "jobs",
                            "workers",
                            stream_id
                        )

                        continue

                    conn.commit()

                    logging.info(
                        f"{reaper_id}: Job {stream_id} "
                        f"marked as processing."
                    )

        except Exception as e:

            logging.exception(
                f"{reaper_id}: Failed to mark "
                f"job {stream_id} as processing. "
                f"Error: {e}"
            )

            continue

        # --------------------------------------------------
        # Simulated processing
        # --------------------------------------------------

        processing_time = random.uniform(0.5, 2)

        logging.info(
            f"{reaper_id}: Processing "
            f"job {stream_id} "
            f"(sleep={processing_time:.2f}s)."
        )

        time.sleep(processing_time)

        status = random.choice(
            ["failed", "completed"]
        )

        # --------------------------------------------------
        # Final status update
        # --------------------------------------------------

        try:
            with get_db() as conn:
                with conn.cursor() as cur:

                    logging.info(
                        f"{reaper_id}: Updating "
                        f"job {stream_id} "
                        f"to status '{status}'."
                    )

                    error_msg = (
                        "Job failed due to simulated error"
                        if status == "failed"
                        else None
                    )

                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = %s,
                            completed_at = NOW(),
                            error = %s
                        WHERE stream_id = %s;
                        """,
                        (
                            status,
                            error_msg,
                            stream_id
                        )
                    )

                    conn.commit()

                    logging.info(
                        f"{reaper_id}: Job {stream_id} "
                        f"marked as {status}."
                    )

        except Exception as e:

            logging.exception(
                f"{reaper_id}: Failed to update "
                f"final status for job {stream_id}. "
                f"Error: {e}"
            )

            continue

        # --------------------------------------------------
        # ACK Redis
        # --------------------------------------------------

        try:

            acked = redis_client.xack(
                "jobs",
                "workers",
                stream_id
                
            )

            logging.info(
                f"{reaper_id}: Job {stream_id} "
                f"acknowledged in Redis. "
                f"xack returned {acked}."
            )

        except Exception as e:

            logging.exception(
                f"{reaper_id}: Failed to ACK "
                f"job {stream_id}. Error: {e}"
            )

    logging.info(
        f"{reaper_id}: Recovery pass complete."
    )