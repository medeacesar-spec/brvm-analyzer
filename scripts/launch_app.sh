#!/bin/bash
# Wrapper script to launch the BRVM Streamlit app
# Called by the launchd agent at 10am local time (= 10am Porto-Novo since both are WAT)

APP_DIR="/Users/mdegbe/brvm-analyzer"
LOG_DIR="$APP_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S %Z")
echo "[$TIMESTAMP] Starting BRVM Analyzer..." >> "$LOG_DIR/launch.log"

# If already running on port 8501, kill it
pkill -f "streamlit run.*app.py.*8501" 2>/dev/null
sleep 1

cd "$APP_DIR" || exit 1

# Launch Streamlit in headless mode, detached
/usr/bin/python3 -m streamlit run app.py \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false \
    >> "$LOG_DIR/streamlit.log" 2>&1 &

STREAMLIT_PID=$!
echo "[$TIMESTAMP] Streamlit started (PID=$STREAMLIT_PID) on http://localhost:8501" >> "$LOG_DIR/launch.log"

# Wait a moment and open browser
sleep 5
/usr/bin/open "http://localhost:8501"

echo "[$TIMESTAMP] Browser opened." >> "$LOG_DIR/launch.log"
