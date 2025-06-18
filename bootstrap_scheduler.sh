#!/bin/bash
# Bootstrap script for climate scheduler
# This small script downloads and executes the full scheduler setup script from GitHub

set -euo pipefail

# Configuration
LOG_FILE="/var/log/climate-bootstrap.log"
REPO_URL="https://github.com/CrazyCreatorr/cuddly-doodle.git"
SCRIPTS_DIR="/opt/climate-scheduler"
REGION="ap-south-1"

# Logging function
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
error_exit() { log "ERROR: $1"; exit 1; }

# Make sure we have required tools
log "Installing minimal requirements..."
apt-get update -y && apt-get install -y git curl awscli jq python3 python3-pip || error_exit "Failed to install minimal requirements"

# Try to clone repository with retries
log "Cloning main repo from GitHub..."
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
    # Look for scheduler setup script
    if [ -f "$SCRIPTS_DIR/scheduler_setup.sh" ]; then
        log "Found scheduler_setup.sh, executing..."
        bash "$SCRIPTS_DIR/scheduler_setup.sh" "$REGION" 2>&1 | tee -a "$LOG_FILE"
    elif [ -f "$SCRIPTS_DIR/aws-infrastructure/terraform/scheduler_setup.sh" ]; then
        log "Found scheduler_setup.sh in aws-infrastructure/terraform/, executing..."
        bash "$SCRIPTS_DIR/aws-infrastructure/terraform/scheduler_setup.sh" "$REGION" 2>&1 | tee -a "$LOG_FILE"
    else
        log "WARNING: Scheduler setup script not found in repo!"
        log "Available files in repo root:"
        ls -la "$SCRIPTS_DIR/" 2>&1 | tee -a "$LOG_FILE" || true
        log "Continuing with minimal setup..."
        
        # Basic minimal setup if script not found
        pip3 install boto3 pytz || log "Failed to install Python packages"
        log "Minimal bootstrap completed - full setup script not available"
    fi
else
    log "WARNING: Failed to clone repository after $max_retries attempts"
    log "Continuing with minimal local setup..."
    
    # Basic setup without repository
    pip3 install boto3 pytz || log "Failed to install Python packages"
    log "Minimal bootstrap completed - repository not available"
fi

log "Bootstrap process completed"
