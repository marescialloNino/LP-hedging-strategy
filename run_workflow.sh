#!/bin/bash

# Base directory
BASE_DIR="$HOME/LP-hedging-strategy"
# Set up logging with absolute path
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"  # Create logs directory upfront
LOG_FILE="$LOG_DIR/workflow_$(date +%Y%m%d_%H%M%S).log"

# Create data directory
DATA_DIR="$BASE_DIR/lp-data"
mkdir -p "$DATA_DIR"

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

# Export environment variables for logging configuration
export LP_HEDGE_LOG_DIR="$LOG_DIR"
export LP_HEDGE_DATA_DIR="$DATA_DIR"

# Start the workflow
log_message "Starting LP hedging workflow..."

# Step 1: Run LP monitor
log_message "Step 1: Running LP monitor..."
cd "$BASE_DIR/lp-monitor"
NODE_ENV=production npm start >> "$LOG_FILE" 2>&1
check_status "LP monitor execution"

# Step 2: Run Bitget position fetcher
log_message "Step 2: Running Bitget position fetcher..."
cd "$BASE_DIR/hedge-monitoring"
python3 bitget_position_fetcher.py >> "$LOG_FILE" 2>&1
check_status "Bitget position fetcher execution"

# Step 3: Run hedge rebalancer
log_message "Step 3: Running hedge rebalancer..."
cd "$BASE_DIR/hedge-rebalancer"
python3 hedge_rebalancer.py >> "$LOG_FILE" 2>&1
check_status "Hedge rebalancer execution"

# Workflow completed
log_message "Workflow completed successfully!"