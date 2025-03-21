#!/bin/bash

# make this file executable with the next command:
# chmod +x run-lp-monitor.sh

# Navigate to the project directory
cd /home/user/dev/lp-monitor  # Replace with actual project path

# Run the compiled JavaScript
node dist/index.js

# Check exit status
if [ $? -eq 0 ]; then
  echo "lp-monitor batch completed successfully."
else
  echo "lp-monitor batch failed."
  exit 1
fi

# ro run cron job (example every 30 minutes):

# crontab -e
# */30 * * * * /home/user/dev/lp-monitor/run-lp-monitor.sh >> /home/user/dev/lp-monitor/lp-monitor.log 2>&1