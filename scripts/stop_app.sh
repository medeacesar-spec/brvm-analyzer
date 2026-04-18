#!/bin/bash
# Stop the BRVM Streamlit app (used manually or by the evening LaunchAgent)

APP_DIR="/Users/mdegbe/brvm-analyzer"
LOG_DIR="$APP_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S %Z")

if pgrep -f "streamlit run.*app.py.*8501" > /dev/null; then
    pkill -f "streamlit run.*app.py.*8501"
    echo "[$TIMESTAMP] Streamlit stopped." >> "$LOG_DIR/launch.log"
else
    echo "[$TIMESTAMP] No Streamlit instance was running." >> "$LOG_DIR/launch.log"
fi
