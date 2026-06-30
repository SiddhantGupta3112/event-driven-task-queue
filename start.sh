#!/bin/bash
uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000} &
UVICORN_PID=$!

python monitor.py &
MONITOR_PID=$!

wait -n $UVICORN_PID $MONITOR_PID