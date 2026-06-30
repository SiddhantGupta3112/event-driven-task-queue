import time
import logging
from multiprocessing import Process
from producer import redis_client, enqueue_job
from worker import consume_jobs, worker_group_exists
from reaper import cleanup_jobs
import uuid
import glob
from datetime import date
import os


STREAM_NAME = "jobs"
GROUP_NAME = "workers"
MAX_WORKERS = 10
MIN_WORKERS = 1
JOBS_PER_WORKER = 5
ADJUST_INTERVAL = 5  

active_worker_processes = []
active_reaper_processes = []

def cleanup_old_reports():
    today_suffix = date.today().isoformat()
    for filepath in glob.glob("/tmp/*_report_*.pdf"):
        if today_suffix not in filepath:
            try:
                os.remove(filepath)
                logging.info(f"Cleaned up stale report: {filepath}")
            except Exception as e:
                logging.error(f"Failed to remove {filepath}: {e}")

def get_stream_lag():
    try:
        groups = redis_client.xinfo_groups(STREAM_NAME)
        for group in groups:
            if group["name"] == GROUP_NAME:
                return group.get("lag", 0)
    except Exception as e:
        logging.error(f"Failed to fetch Redis stream info: {e}")
    return 0

def run_workers(worker_id: str):
    try:
        consume_jobs(worker_id=worker_id, max_idle_polls=3)
    except Exception as e:
        logging.exception(f"{worker_id}: Worker crashed with error: {e}")

def run_reapers(reaper_id: str):
    try:
        cleanup_jobs(reaper_id=reaper_id)
    except Exception as e:
        logging.exception(f"{reaper_id}: Reaper crashed with error: {e}")

def adjust_workers():
    global active_worker_processes
    active_worker_processes = [p for p in active_worker_processes if p.is_alive()]
    current_worker_count = len(active_worker_processes)

    lag = get_stream_lag()
    ideal_worker_count = max(MIN_WORKERS, min(MAX_WORKERS, lag // JOBS_PER_WORKER))

    logging.info(
        f"Worker scaling | Lag={lag} | Active={current_worker_count} | Target={ideal_worker_count}"
    )

    if current_worker_count < ideal_worker_count:
        workers_to_spawn = ideal_worker_count - current_worker_count
        for _ in range(workers_to_spawn):
            worker_id = f"worker-{ uuid.uuid4().hex[:8]}"
            p = Process(target=run_workers, args=(worker_id,))
            p.start()
            active_worker_processes.append(p)
            logging.info(f"Spawned {worker_id}")

def adjust_reapers():
    global active_reaper_processes
    active_reaper_processes = [p for p in active_reaper_processes if p.is_alive()]
    current_reaper_count = len(active_reaper_processes)

    try:
        pending_info = redis_client.xpending(STREAM_NAME, GROUP_NAME)
        pending_count = pending_info.get("count", 0)
    except Exception as e:
        logging.error(f"Failed to fetch Redis pending info: {e}")
        pending_count = 0

    ideal_reaper_count = max(MIN_WORKERS, min(MAX_WORKERS, pending_count // JOBS_PER_WORKER))

    logging.info(
        f"Reaper scaling | Pending={pending_count} | Active={current_reaper_count} | Target={ideal_reaper_count}"
    )

    if current_reaper_count < ideal_reaper_count:
        reapers_to_spawn = ideal_reaper_count - current_reaper_count
        for _ in range(reapers_to_spawn):
            reaper_id = f"reaper-{uuid.uuid4().hex[:8]}"
            p = Process(target=run_reapers, args=(reaper_id,))
            p.start()
            active_reaper_processes.append(p)
            logging.info(f"Spawned {reaper_id}")
            
 
def enqueue_demo_jobs(n=20):
    """Enqueue n demo jobs using the producer pipeline."""
    for i in range(n):
        payload = {"task_type": "demo", "index": i}
        try:
            stream_id = enqueue_job(payload)
            logging.info(f"Enqueued job {i} with stream_id={stream_id}")
        except Exception as e:
            logging.error(f"Failed to enqueue job {i}: {e}")           

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("Controller starting...")
    
    worker_group_exists()

    # Step 1: enqueue 20 jobs
    #enqueue_demo_jobs(20)

    # Step 2: start monitor loop
    try:
        while True:
            adjust_workers()
            adjust_reapers()
            cleanup_old_reports()
            time.sleep(ADJUST_INTERVAL)
    except KeyboardInterrupt:
        logging.info("Controller shutting down...")
        for p in active_worker_processes + active_reaper_processes:
            p.join(timeout=30)
            if p.is_alive():
                logging.info(f"Terminating {p.pid}")
                p.terminate()
