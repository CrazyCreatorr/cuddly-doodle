#!/bin/bash
# Bootstrap script for climate processor instances
# This downloads and executes the full processor script from GitHub

set -euo pipefail

# Configuration
LOG_FILE="/var/log/processor-bootstrap.log"
REPO_URL="https://github.com/CrazyCreatorr/cuddly-doodle.git"
SCRIPTS_DIR="/opt/climate-processor"

# Logging function
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
error_exit() { log "ERROR: $1"; exit 1; }

log "=== Processor Bootstrap Started ==="

# Install minimal requirements
log "Installing minimal requirements..."
apt-get update -y && apt-get install -y git curl awscli jq python3 python3-pip || error_exit "Failed to install minimal requirements"

# Clone repository with retries
log "Cloning processor scripts from GitHub..."
mkdir -p "$SCRIPTS_DIR"
clone_success=false
max_retries=3

for i in $(seq 1 $max_retries); do
    log "Clone attempt $i of $max_retries..."
    if git clone "$REPO_URL" "$SCRIPTS_DIR"; then
        log "Successfully cloned repository"
        chmod -R 755 "$SCRIPTS_DIR"
        clone_success=true
        break
    else
        log "Clone attempt $i failed"
        rm -rf "$SCRIPTS_DIR"
        mkdir -p "$SCRIPTS_DIR"
        sleep 10
    fi
done

if [ "$clone_success" = true ]; then
    # Look for processor script in repository
    if [ -f "$SCRIPTS_DIR/user_data_processor.sh" ]; then
        log "Found user_data_processor.sh in repo root, executing..."
        bash "$SCRIPTS_DIR/user_data_processor.sh" 2>&1 | tee -a "$LOG_FILE"
    elif [ -f "$SCRIPTS_DIR/aws-infrastructure/terraform/user_data_processor.sh" ]; then
        log "Found user_data_processor.sh in aws-infrastructure/terraform/, executing..."
        bash "$SCRIPTS_DIR/aws-infrastructure/terraform/user_data_processor.sh" 2>&1 | tee -a "$LOG_FILE"
    else
        log "ERROR: Processor script not found in repository!"
        log "Available files in repo root:"
        ls -la "$SCRIPTS_DIR/" 2>&1 | tee -a "$LOG_FILE" || true
        error_exit "Cannot proceed without processor script"
    fi
else
    error_exit "Failed to clone repository after $max_retries attempts"
fi

log "=== Processor Bootstrap Completed ==="
