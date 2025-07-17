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

# Initialize Conda (using your Miniforge3 installation path)
CONDA_BASE="/home/cricri/miniforge3"
source "$CONDA_BASE/etc/profile.d/conda.sh"
check_status "Conda initialization"

# Start the workflow
log_message "Starting LP hedging workflow..."

# Step 1: Run LP monitor (Node.js, no Conda needed)
log_message "Step 1: Running LP monitor..."
cd "$BASE_DIR/lp-monitor"
NODE_ENV=production npm start >> "$LOG_FILE" 2>&1
check_status "LP monitor execution"

# Activate the Conda environment 'stat39'
CONDA_ENV="stat39"
log_message "Activating Conda environment: $CONDA_ENV"
conda activate "$CONDA_ENV"
check_status "Conda environment activation"

# Step 2: Run Bitget position fetcher (using stat39 env)
log_message "Step 2: Running Bitget position fetcher..."
cd "$BASE_DIR/python"
python3 -m hedge-monitoring.bitget_position_fetcher >> "$LOG_FILE" 2>&1
check_status "Bitget position fetcher execution"

# Step 3: Run hedge rebalancer (using stat39 env)
log_message "Step 3: Running hedge rebalancer..."
cd "$BASE_DIR/python"
python3 -m  hedge-rebalancer.hedge_rebalancer >> "$LOG_FILE" 2>&1
check_status "Hedge rebalancer execution"

# Deactivate Conda environment (optional)
log_message "Deactivating Conda environment"
conda deactivate

# Workflow completed
log_message "Workflow completed successfully!"
