#!/bin/bash

# Set up logging with absolute path
LOG_DIR="$HOME/LP-hedging-strategy/logs"
mkdir -p "$LOG_DIR"  # Create logs directory upfront
LOG_FILE="$LOG_DIR/workflow_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if a command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        log_message "SUCCESS: $1"
    else
        log_message "ERROR: $1"
        exit 1
    fi
}

# Start the workflow
log_message "Starting LP hedging workflow..."

# Create necessary directories (lp-data still needed)
mkdir -p "$HOME/LP-hedging-strategy/lp-data"

# Step 1: Run LP monitor
log_message "Step 1: Running LP monitor..."
cd "$HOME/LP-hedging-strategy/lp-monitor"
npm start >> "$LOG_FILE" 2>&1
check_status "LP monitor execution"

# Step 2: Run Bitget position fetcher
log_message "Step 2: Running Bitget position fetcher..."
cd "$HOME/LP-hedging-strategy/hedge-monitoring"
python3 bitget_position_fetcher.py >> "$LOG_FILE" 2>&1
check_status "Bitget position fetcher execution"

# Step 3: Run hedge rebalancer
log_message "Step 3: Running hedge rebalancer..."
cd "$HOME/LP-hedging-strategy/hedge-rebalancer"
python3 hedge_rebalancer.py >> "$LOG_FILE" 2>&1
check_status "Hedge rebalancer execution"

# Workflow completed
log_message "Workflow completed successfully!"