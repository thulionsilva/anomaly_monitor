#!/bin/bash

# Ensure the script exits on failure
set -e

# Start the cron service
service cron start

# Check if the model file exists; if not, train the model
[ -f /src/models/PIC11151A.pkl ] || python3 /src/train.py

# Remove all existing cron jobs to avoid duplicates
crontab -r 2>/dev/null || true

INTERVAL=$CRON_INTERVAL
# Ensure CRON_INTERVAL is defined
if [ -z "$CRON_INTERVAL" ]; then
  INTERVAL="*/15 * * * *"
fi

# Define the command to be executed
COMMAND="/opt/conda/bin/python3 /src/predict.py > /src/crontab/predict.log 2>&1"

# Create the log directory
mkdir -p /src/crontab

# Add the cron job
(crontab -l 2>/src/crontab/log || true; echo "$INTERVAL $COMMAND") | crontab -

# List the current cron jobs
echo "Current cron jobs:"
crontab -l


# Keep the container running
 tail -f /dev/null
