#!/bin/bash

# Self-contained processor user_data script that clones pipeline repository
set -euo pipefail

# Configuration
LOG_FILE="/var/log/user-data-processor.log"
REGION="ap-south-1"
REPO_URL="https://github.com/CrazyCreatorr/cuddly-doodle.git"
SCRIPTS_DIR="/opt/climate-processor"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Error handling
error_exit() {
    log "ERROR: $1"
    
    # Get instance ID for tagging
    local instance_id=$(curl -s http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)
    
    if [ ! -z "$instance_id" ]; then
        # Tag instance as failed
        aws ec2 create-tags \
            --region "$REGION" \
            --resources "$instance_id" \
            --tags Key=ProcessingStatus,Value=Failed Key=FailedAt,Value="$(date -u +%Y-%m-%dT%H:%M:%SZ)" Key=ErrorMessage,Value="$1" 2>/dev/null || \
            log "Warning: Failed to tag instance as failed"
        
        # Terminate the failed instance after a short delay
        log "Terminating failed instance in 60 seconds..."
        sleep 60
        aws ec2 terminate-instances --region "$REGION" --instance-ids "$instance_id" 2>/dev/null || {
            log "Failed to terminate via API, falling back to shutdown"
            shutdown -h now
        }
    fi
    
    exit 1
}

# Install required packages
install_packages() {
    log "Installing required packages..."
    apt-get update -y || error_exit "Failed to update system"
    apt-get install -y python3 python3-pip git awscli jq curl gdal-bin libgdal-dev python3-gdal nodejs npm || error_exit "Failed to install packages"
    
    # Install essential Python packages with version constraints based on requirements.txt files
    log "Installing Python packages..."
    pip3 install boto3 pytz \
        netcdf4>=1.6.0 \
        xarray>=2023.1.0 \
        geopandas>=0.12.0 \
        rasterio \
        fiona>=1.8.0 \
        shapely>=2.0.0 \
        folium \
        requests>=2.28.0 \
        tqdm \
        global-land-mask>=1.0.0 \
        pandas>=1.5.0 \
        numpy>=1.24.0 \
        cdsapi>=0.6.1 || error_exit "Failed to install Python packages"
    
    # Install tippecanoe for MBTiles generation
    log "Installing tippecanoe..."
    git clone https://github.com/mapbox/tippecanoe.git /tmp/tippecanoe
    cd /tmp/tippecanoe
    make -j$(nproc)
    make install
    cd /
    rm -rf /tmp/tippecanoe
    
    # Install mbutil for MBTiles extraction
    log "Installing mb-util..."
    pip3 install mbutil
    npm install -g @mapbox/mbtiles
    
    # Install the install-mbutil.sh script if found in repo
    if [ -f "$SCRIPTS_DIR/install-mbutil.sh" ]; then
        log "Found install-mbutil.sh in repo, executing..."
        chmod +x "$SCRIPTS_DIR/install-mbutil.sh"
        "$SCRIPTS_DIR/install-mbutil.sh"
    fi
    pip3 install mbutil
}

# Clone and setup pipeline scripts from GitHub repository
setup_pipeline_scripts() {
    log "Cloning pipeline scripts from GitHub repository..."
    
    # Clone the repository
    if git clone "$REPO_URL" "$SCRIPTS_DIR"; then
        log "Successfully cloned pipeline repository"
        
        # Set proper permissions
        chmod -R 755 "$SCRIPTS_DIR"
        chown -R ubuntu:ubuntu "$SCRIPTS_DIR"
        
        # Change to the repository directory
        cd "$SCRIPTS_DIR"
        
        log "Repository contents:"
        ls -la | tee -a "$LOG_FILE"
        
    else
        error_exit "Failed to clone pipeline repository from $REPO_URL"
    fi
}

# Create or discover S3 buckets for storing processed data
setup_s3_buckets() {
    log "Setting up S3 buckets for climate data storage..."
    
    # Discover existing aqua-hive buckets (they should already exist from Terraform)
    local raw_bucket
    raw_bucket=$(aws s3api list-buckets \
        --query 'Buckets[?contains(Name, `aqua-hive-raw-data`)].Name' \
        --output text 2>/dev/null | head -1)
    
    if [ -z "$raw_bucket" ]; then
        # Fallback to any raw data bucket pattern
        raw_bucket=$(aws s3api list-buckets \
            --query 'Buckets[?contains(Name, `raw-data`) || contains(Name, `climate-raw`)].Name' \
            --output text 2>/dev/null | head -1)
    fi
    
    if [ -z "$raw_bucket" ]; then
        log "WARNING: No raw data bucket found. Data upload may fail."
        raw_bucket="aqua-hive-raw-data-$(date +%Y%m%d)"
    fi
    
    # Get tiles bucket
    local tiles_bucket
    tiles_bucket=$(aws s3api list-buckets \
        --query 'Buckets[?contains(Name, `aqua-hive-tiles`)].Name' \
        --output text 2>/dev/null | head -1)
    
    if [ -z "$tiles_bucket" ]; then
        # Fallback to any tiles bucket pattern
        tiles_bucket=$(aws s3api list-buckets \
            --query 'Buckets[?contains(Name, `tiles`) || contains(Name, `mbtiles`)].Name' \
            --output text 2>/dev/null | head -1)
    fi
    
    if [ -z "$tiles_bucket" ]; then
        log "WARNING: No tiles bucket found. Tile upload may fail."
        tiles_bucket="aqua-hive-tiles-$(date +%Y%m%d)"
    fi
    
    # Export bucket names for use by pipeline scripts
    export CLIMATE_RAW_BUCKET="$raw_bucket"
    export CLIMATE_TILES_BUCKET="$tiles_bucket"
    
    log "Using buckets - Raw: $raw_bucket, Tiles: $tiles_bucket"
}

# Run the climate data processing pipelines
run_processor() {
    log "Starting climate data processing..."
    
    # Ensure we're in the scripts directory
    if [ ! -d "$SCRIPTS_DIR" ]; then
        error_exit "Scripts directory $SCRIPTS_DIR does not exist"
    fi
    
    log "Changing to directory: $SCRIPTS_DIR"
    cd "$SCRIPTS_DIR" || error_exit "Failed to change to scripts directory"
    
    log "Current directory: $(pwd)"
    log "Contents of current directory:"
    ls -la | tee -a "$LOG_FILE"
    
    # Define the parameters to process (temperature, humidity, precipitation)
    local script_params=("temperature" "humidity" "precipitation")
    
    log "Starting processing loop for parameters: ${script_params[*]}"
    
    for param in "${script_params[@]}"; do
        log "Processing $param data..."
        
        # Look for the main pipeline script at the root of the repository or in parameter directory
        local pipeline_script=""
        
        # First check if script exists in parameter directory
        if [ -f "${param}/${param}_pipeline.py" ]; then
            pipeline_script="${param}/${param}_pipeline.py"
            log "Found pipeline script in parameter directory: $pipeline_script"
        # Then check if it exists in the repository root
        elif [ -f "${param}_pipeline.py" ]; then
            pipeline_script="${param}_pipeline.py"
            log "Found pipeline script in repository root: $pipeline_script"
        # Look for scripts in any subdirectory (fallback method)
        else
            log "Searching for ${param}_pipeline.py in subdirectories..."
            local found_script=$(find . -name "${param}_pipeline.py" | head -1)
            if [ ! -z "$found_script" ]; then
                pipeline_script="$found_script"
                log "Found pipeline script in subdirectory: $pipeline_script"
            fi
        fi
        
        if [ ! -z "$pipeline_script" ]; then
            log "Executing pipeline script: $pipeline_script"
            
            # Make script executable and run it
            chmod +x "$pipeline_script" 2>/dev/null || true
            
            if python3 "$pipeline_script" 2>&1 | tee -a "$LOG_FILE"; then
                log "Successfully processed $param data"
                
                # If MBTiles were generated, upload them to S3
                if [ ! -z "${CLIMATE_TILES_BUCKET:-}" ]; then
                    log "Looking for generated MBTiles for $param..."
                    local mbtiles_files=$(find . -name "*${param}*.mbtiles" -o -name "*${param}*.zip" 2>/dev/null)
                    if [ ! -z "$mbtiles_files" ]; then
                        log "Found MBTiles files for $param, uploading to S3..."
                        for mbtile_file in $mbtiles_files; do
                            log "Uploading $mbtile_file to s3://${CLIMATE_TILES_BUCKET}/"
                            aws s3 cp "$mbtile_file" "s3://${CLIMATE_TILES_BUCKET}/" || log "Warning: Failed to upload $mbtile_file"
                        done
                    fi
                    
                    # Look for tiles in expected output directories
                    for tiles_dir in "${param}_mbtiles_output" "${param}_tiles" "tiles"; do
                        if [ -d "$tiles_dir" ]; then
                            log "Found tiles directory: $tiles_dir, uploading contents..."
                            aws s3 sync "$tiles_dir" "s3://${CLIMATE_TILES_BUCKET}/${param}/" || log "Warning: Failed to sync $tiles_dir"
                            
                            # Clean up local tiles to save space
                            local temp_tiles_dir="/tmp/${param}_tiles_$(date +%s)"
                            if mv "$tiles_dir" "$temp_tiles_dir" 2>/dev/null; then
                                log "Moved $tiles_dir to temporary location for cleanup"
                                rm -rf "$temp_tiles_dir"
                            fi
                        fi
                    done
                fi
            else
                log "ERROR: Failed to process $param data"
            fi
        else
            log "WARNING: No pipeline script found for $param"
            continue
        fi
    done
    
    log "All processing completed successfully!"
    
    # Send completion notification to CloudWatch logs
    log "Sending completion notification..."
    
    # Get instance ID for tagging
    local instance_id=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
    
    # Tag instance as completed
    aws ec2 create-tags \
        --region "$REGION" \
        --resources "$instance_id" \
        --tags Key=ProcessingStatus,Value=Completed Key=CompletedAt,Value="$(date -u +%Y-%m-%dT%H:%M:%SZ)" || \
        log "Warning: Failed to tag instance as completed"
    
    # Create a completion marker file
    echo "Processing completed at $(date)" > /tmp/processing_complete
    
    # Wait a short time for any final operations
    sleep 30
    
    # Terminate the instance immediately (instead of shutdown which is slower)
    log "Processing completed. Terminating instance immediately..."
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$instance_id" || {
        log "Failed to terminate via API, falling back to shutdown"
        shutdown -h now
    }
}

# Main execution
main() {
    log "=== Processor Instance Setup Started ==="
    log "Script directory: $SCRIPTS_DIR"
    log "Region: $REGION"
    log "Repository URL: $REPO_URL"
    
    # Install packages first
    log "Step 1: Installing packages..."
    install_packages
    
    # Setup pipeline scripts
    log "Step 2: Setting up pipeline scripts..."
    setup_pipeline_scripts
    
    # Setup S3 buckets
    log "Step 3: Setting up S3 buckets..."
    setup_s3_buckets
    
    # Run the actual processing
    log "Step 4: Running processor..."
    run_processor
    
    log "=== Processor Instance Setup Completed ==="
}

# Run main function
main 2>&1 | tee -a "$LOG_FILE"
