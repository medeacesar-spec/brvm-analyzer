#!/bin/bash
# Kill any running Streamlit and relaunch fresh

APP_DIR="/Users/mdegbe/brvm-analyzer"
LOG_DIR="$APP_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S %Z")
echo "[$TIMESTAMP] Restart requested..." >> "$LOG_DIR/launch.log"

# Kill existing instance on port 8501
pkill -f "streamlit run.*app.py.*8501" 2>/dev/null
sleep 1

cd "$APP_DIR" || exit 1

/usr/bin/python3 -m streamlit run app.py \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false \
    >> "$LOG_DIR/streamlit.log" 2>&1 &

echo "[$TIMESTAMP] Streamlit restarted (PID=$!)" >> "$LOG_DIR/launch.log"

sleep 4
/usr/bin/open "http://localhost:8501"
