#!/bin/bash

# restart_api.sh
# Safely restarts the uvicorn process for the API with robust error handling.

set -e

# Configuration with environment variable overrides and sensible defaults
APP_MODULE="${APP_MODULE:-app.main:app}"
API_HOST="${API_HOST:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
LOG_FILE="${LOG_FILE:-uvicorn.log}"

echo "=== KyroDB API Restart Script ==="
echo "App Module: $APP_MODULE"
echo "Host: $API_HOST"
echo "Port: $API_PORT"
echo "Log File: $LOG_FILE"

# Validate uvicorn is available
if ! command -v uvicorn &> /dev/null; then
    echo "ERROR: uvicorn not found in PATH"
    echo "Please install uvicorn: pip install uvicorn"
    exit 1
fi

echo "Stopping uvicorn process..."

# Find PID of uvicorn process
PID=$(pgrep -f "uvicorn.*${APP_MODULE}" || true)

if [ -z "$PID" ]; then
    echo "No uvicorn process found running."
else
    echo "Found uvicorn process with PID: $PID"
    kill -TERM "$PID"
    
    # Wait for process to exit
    TIMEOUT=10
    COUNT=0
    while kill -0 "$PID" 2>/dev/null; do
        if [ "$COUNT" -ge "$TIMEOUT" ]; then
            echo "Process did not exit after $TIMEOUT seconds. Force killing..."
            kill -KILL "$PID"
            break
        fi
        sleep 1
        COUNT=$((COUNT+1))
    done
    echo "Process stopped."
fi

# Wait for port to be released
sleep 2

echo "Starting uvicorn process..."
# Start uvicorn with quoted variable expansions to avoid shell expansion issues
nohup uvicorn "$APP_MODULE" --host "$API_HOST" --port "$API_PORT" > "$LOG_FILE" 2>&1 &

NEW_PID=$!
echo "Started uvicorn with PID: $NEW_PID"

# Verify startup by checking if port is actually listening
echo "Verifying API startup..."
sleep 3

# Check if port is listening (more reliable than PID check)
if command -v curl &> /dev/null; then
    # Try health check endpoint if available
    if curl -sf "http://localhost:${API_PORT}/health" &> /dev/null; then
        echo "✓ Uvicorn started successfully (health check passed)"
        exit 0
    elif curl -sf "http://localhost:${API_PORT}/" &> /dev/null; then
        echo "✓ Uvicorn started successfully (port responding)"
        exit 0
    else
        echo "✗ Port ${API_PORT} not responding after startup"
        echo "Check $LOG_FILE for details"
        tail -n 20 "$LOG_FILE"
        exit 1
    fi
elif command -v nc &> /dev/null; then
    # Fallback to netcat port check
    if nc -z localhost "$API_PORT" 2>/dev/null; then
        echo "✓ Uvicorn started successfully (port open)"
        exit 0
    else
        echo "✗ Port ${API_PORT} not listening after startup"
        echo "Check $LOG_FILE for details"
        tail -n 20 "$LOG_FILE"
        exit 1
    fi
else
    # Fallback to PID check (least reliable)
    if kill -0 "$NEW_PID" 2>/dev/null; then
        echo "⚠ Uvicorn process running (PID check), but cannot verify port"
        echo "Install curl or nc for better verification"
        exit 0
    else
        echo "✗ Failed to start uvicorn"
        echo "Check $LOG_FILE for details"
        tail -n 20 "$LOG_FILE"
        exit 1
    fi
fi
