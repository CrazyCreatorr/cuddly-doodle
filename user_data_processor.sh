#!/bin/bash

# Self-contained processor user_data script that clones pipeline repository
set -euo pipefail

# Configuration
LOG_FILE="/var/log/user-data-processor.log"
REGION="ap-south-1"
REPO_URL="https://github.com/CrazyCreat        log "Processing $param data..."
        
        # Look for the main pipeline script in repository root, parameter-specific directory, or any subdirectory
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
        fioodle.git"
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
        
        # Make all shell scripts executable
        find "$SCRIPTS_DIR" -name "*.sh" -type f -exec chmod +x {} \;
        
        log "Pipeline scripts setup completed successfully"
        log "Available contents:"
        ls -la "$SCRIPTS_DIR"/ | tee -a "$LOG_FILE"
    else
        error_exit "Failed to clone pipeline repository"
    fi
}

# Get S3 bucket names dynamically and create if they don't exist
get_s3_buckets() {
    log "Discovering S3 buckets..."
    
    # Generate unique bucket name suffix based on account ID
    local account_id
    account_id=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null || echo "default")
    local bucket_suffix="-${account_id:(-6)}"
    
    # Get raw data bucket
    local raw_bucket
    raw_bucket=$(aws s3api list-buckets \
        --query 'Buckets[?contains(Name, `climate-raw-data`) || contains(Name, `raw-data`)].Name' \
        --output text 2>/dev/null | head -1)
    
    if [ -z "$raw_bucket" ]; then
        raw_bucket=$(aws s3api list-buckets \
            --query 'Buckets[?contains(Name, `aqua-hive-raw`)].Name' \
            --output text 2>/dev/null | head -1)
    fi
    
    # Create raw bucket if it doesn't exist
    if [ -z "$raw_bucket" ]; then
        log "Raw data bucket not found, creating one..."
        raw_bucket="climate-raw-data${bucket_suffix}"
        
        # Check if the bucket name is available
        if aws s3api head-bucket --bucket "$raw_bucket" 2>/dev/null; then
            # Bucket exists but we don't have access - add timestamp to make unique
            raw_bucket="climate-raw-data-$(date +%Y%m%d)${bucket_suffix}"
        fi
        
        # Create the bucket
        if aws s3 mb "s3://$raw_bucket" --region "$REGION"; then
            # Set lifecycle policy to transition objects to Glacier after 30 days and DEEP_ARCHIVE after 120 days
            log "Setting lifecycle policy for raw data bucket..."
            aws s3api put-bucket-lifecycle-configuration \
                --bucket "$raw_bucket" \
                --lifecycle-configuration '{
                    "Rules": [
                        {
                            "ID": "Archive Transitions",
                            "Status": "Enabled",
                            "Prefix": "",
                            "Transitions": [
                                {
                                    "Days": 30,
                                    "StorageClass": "GLACIER"
                                },
                                {
                                    "Days": 120,
                                    "StorageClass": "DEEP_ARCHIVE"
                                }
                            ]
                        }
                    ]
                }'
            log "Raw data bucket created: $raw_bucket"
        else
            error_exit "Failed to create raw data bucket"
        fi
    fi
    
    # Get tiles bucket
    local tiles_bucket
    tiles_bucket=$(aws s3api list-buckets \
        --query 'Buckets[?contains(Name, `climate-tiles`) || contains(Name, `mbtiles`)].Name' \
        --output text 2>/dev/null | head -1)
    
    if [ -z "$tiles_bucket" ]; then
        tiles_bucket=$(aws s3api list-buckets \
            --query 'Buckets[?contains(Name, `aqua-hive-tiles`)].Name' \
            --output text 2>/dev/null | head -1)
    fi
    
    # Create tiles bucket if it doesn't exist
    if [ -z "$tiles_bucket" ]; then
        log "Tiles bucket not found, creating one..."
        tiles_bucket="climate-tiles${bucket_suffix}"
        
        # Check if the bucket name is available
        if aws s3api head-bucket --bucket "$tiles_bucket" 2>/dev/null; then
            # Bucket exists but we don't have access - add timestamp to make unique
            tiles_bucket="climate-tiles-$(date +%Y%m%d)${bucket_suffix}"
        fi
        
        # Create the bucket
        if aws s3 mb "s3://$tiles_bucket" --region "$REGION"; then
            # Set CORS policy for web access
            log "Setting CORS policy for tiles bucket..."
            aws s3api put-bucket-cors \
                --bucket "$tiles_bucket" \
                --cors-configuration '{
                    "CORSRules": [
                        {
                            "AllowedHeaders": ["*"],
                            "AllowedMethods": ["GET"],
                            "AllowedOrigins": ["*"],
                            "MaxAgeSeconds": 3000
                        }
                    ]
                }'
            log "Tiles bucket created: $tiles_bucket"
        else
            error_exit "Failed to create tiles bucket"
        fi
    fi
    
    log "Found S3 buckets - Raw: $raw_bucket, Tiles: $tiles_bucket"
    echo "$raw_bucket|$tiles_bucket"
}

# Main processor logic
run_processor() {
    log "Starting processor setup..."
    
    # Get S3 bucket names
    local bucket_info
    bucket_info=$(get_s3_buckets)
    IFS='|' read -r raw_bucket tiles_bucket <<< "$bucket_info"
    
    # Set environment variables for the processing scripts
    export RAW_BUCKET="$raw_bucket"
    export TILES_BUCKET="$tiles_bucket"
    export AWS_REGION="$REGION"
    
    # Change to the cloned repository directory
    cd "$SCRIPTS_DIR"
    
    log "Repository structure:"
    find . -type f -name "*.py" -o -name "*.sh" | head -20 | tee -a "$LOG_FILE"
    
    # Check if pipeline scripts exist and execute them
    local script_params=("temperature" "humidity" "precipitation")
    
    for param in "${script_params[@]}"; do
        log "Processing $param data..."
        
        # Look for the main pipeline script at the root of the repository or in parameter directory
        local pipeline_script=""
        
        # First check if script exists in parameter directory
        if [ -f "${param}/${param}_pipeline.py" ]; then
            pipeline_script="${param}/${param}_pipeline.py"
        # Then check if it exists in the repository root
        elif [ -f "${param}_pipeline.py" ]; then
            pipeline_script="${param}_pipeline.py"
        fi
        
        if [ ! -z "$pipeline_script" ]; then
            log "Found pipeline script: $pipeline_script"
            
            # Check for requirements.txt in parameter directory or root
            if [ -f "${param}/requirements.txt" ]; then
                log "Installing dependencies from ${param}/requirements.txt"
                pip3 install -r "${param}/requirements.txt"
            elif [ -f "requirements.txt" ]; then
                log "Installing dependencies from root requirements.txt"
                pip3 install -r "requirements.txt"
            fi
            
            log "Executing $pipeline_script for $param processing..."
                
            # Execute the pipeline script
            cd "$SCRIPTS_DIR" # Ensure we're in the correct directory
            if python3 "$pipeline_script"; then
                log "Successfully processed $param data"
                
                # Upload any generated data to S3
                log "Uploading $param data to S3..."
                
                # Upload CSV files to raw bucket - check multiple possible output directories
                log "Looking for CSV output files to upload to S3..."
                for data_dir in ${param}_data_output *_data_output ${param}/data_output ${param}/${param}_data_output; do
                    if [ -d "$data_dir" ] && ls $data_dir/*.csv 2>/dev/null; then
                        log "Found CSV files in $data_dir, uploading to S3..."
                        aws s3 sync $data_dir/ "s3://$raw_bucket/$param/$(date +%Y)/$(date +%m)/" \
                            --exclude "*" --include "*.csv" --storage-class GLACIER
                    fi
                done
                
                # Upload MBTiles to tiles bucket as individual tiles - check multiple possible output directories
                log "Looking for MBTiles files to upload to S3..."
                local mbtiles_found=false
                for mbtiles_dir in ${param}_mbtiles_output *_mbtiles_output ${param}/mbtiles_output ${param}/${param}_mbtiles_output; do
                    if [ -d "$mbtiles_dir" ] && ls $mbtiles_dir/*.mbtiles 2>/dev/null; then
                        mbtiles_found=true
                        log "Found MBTiles in $mbtiles_dir"
                    fi
                done
                
                if $mbtiles_found; then
                    for mbtiles_file in ${param}_mbtiles_output/*.mbtiles *_mbtiles_output/*.mbtiles ${param}/mbtiles_output/*.mbtiles ${param}/${param}_mbtiles_output/*.mbtiles 2>/dev/null; do
                        if [ ! -f "$mbtiles_file" ]; then continue; fi
                        log "Extracting tiles from $mbtiles_file"
                            
                            # Create temporary directory for tile extraction
                            local temp_tiles_dir="/tmp/tiles_${param}_$(date +%s)"
                            mkdir -p "$temp_tiles_dir"
                            
                            # Extract tiles using mb-util if available, otherwise use Python
                            if command -v mb-util &> /dev/null; then
                                mb-util "$mbtiles_file" "$temp_tiles_dir" --image_format=pbf
                            else
                                # Use Python to extract tiles
                                python3 -c "
import sqlite3
import os
from pathlib import Path

def extract_mbtiles_to_pbf(mbtiles_path, output_dir):
    conn = sqlite3.connect('$mbtiles_file')
    cursor = conn.cursor()
    cursor.execute('SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles')
    
    for row in cursor.fetchall():
        zoom, x, y, data = row
        tile_dir = Path('$temp_tiles_dir') / str(zoom) / str(x)
        tile_dir.mkdir(parents=True, exist_ok=True)
        
        tile_path = tile_dir / f'{y}.pbf'
        with open(tile_path, 'wb') as f:
            f.write(data)
    
    conn.close()

extract_mbtiles_to_pbf('$mbtiles_file', '$temp_tiles_dir')
"
                            fi
                            
                            # Upload extracted tiles to S3
                            if [ -d "$temp_tiles_dir" ]; then
                                aws s3 sync "$temp_tiles_dir" "s3://$tiles_bucket/$param/$(date +%Y)/$(date +%m)/" \
                                    --content-type "application/x-protobuf" \
                                    --cache-control "max-age=2592000"
                                
                                # Clean up temporary directory
                                rm -rf "$temp_tiles_dir"
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
    
    install_packages
    setup_pipeline_scripts
    run_processor
    
    log "=== Processor Instance Setup Completed ==="
}

# Run main function
main 2>&1 | tee -a "$LOG_FILE"
