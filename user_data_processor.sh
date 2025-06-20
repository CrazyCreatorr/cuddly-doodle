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
        zarr \
        fsspec \
        s3fs \
        netcdf4 \
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
    log "Setting up pipeline scripts..."
    
    # Check if directory already exists (from bootstrap)
    if [ -d "$SCRIPTS_DIR" ]; then
        log "Scripts directory already exists, updating..."
        cd "$SCRIPTS_DIR"
        
        # Pull latest changes
        if git pull origin main; then
            log "Successfully updated pipeline repository"
        else
            log "Failed to pull updates, continuing with existing scripts..."
        fi
    else
        log "Cloning pipeline scripts from GitHub repository..."
        # Clone the repository
        if git clone "$REPO_URL" "$SCRIPTS_DIR"; then
            log "Successfully cloned pipeline repository"
            cd "$SCRIPTS_DIR"
        else
            error_exit "Failed to clone pipeline repository from $REPO_URL"
        fi
    fi
    
    # Set proper permissions
    chmod -R 755 "$SCRIPTS_DIR"
    chown -R ubuntu:ubuntu "$SCRIPTS_DIR"
    
    # Create output directories with proper permissions
    log "Creating output directories..."
    mkdir -p "$SCRIPTS_DIR/temperature_data_output" "$SCRIPTS_DIR/temperature_mbtiles_output"
    mkdir -p "$SCRIPTS_DIR/humidity_data_output" "$SCRIPTS_DIR/humidity_mbtiles_output"  
    mkdir -p "$SCRIPTS_DIR/precipitation_data_output" "$SCRIPTS_DIR/precipitation_mbtiles_output"
    
    # Set permissions for output directories
    chown -R ubuntu:ubuntu "$SCRIPTS_DIR"/*_output
    chmod -R 755 "$SCRIPTS_DIR"/*_output
    
    log "Repository contents:"
    ls -la | tee -a "$LOG_FILE"
}

# Create or discover S3 buckets for storing processed data
setup_s3_buckets() {
    log "Setting up S3 buckets for climate data storage..."
    
    # Use hardcoded bucket names that we know exist from Terraform
    # These buckets are created by Terraform with specific naming pattern
    local raw_bucket="aqua-hive-raw-data-82zp4fuh"
    local tiles_bucket="aqua-hive-tiles-82zp4fuh"
    
    # Verify buckets exist (with timeout to prevent hanging)
    log "Verifying buckets exist..."
    
    if timeout 30 aws s3 ls "s3://$tiles_bucket/" >/dev/null 2>&1; then
        log "Tiles bucket verified: $tiles_bucket"
    else
        log "WARNING: Tiles bucket $tiles_bucket not accessible, trying to discover..."
        # Quick fallback discovery with timeout
        tiles_bucket=$(timeout 15 aws s3api list-buckets \
            --query 'Buckets[?contains(Name, `tiles`)].Name' \
            --output text 2>/dev/null | head -1)
        
        if [ -z "$tiles_bucket" ]; then
            log "ERROR: No tiles bucket found"
            tiles_bucket="aqua-hive-tiles-82zp4fuh"  # fallback to expected name
        fi
    fi
    
    if timeout 30 aws s3 ls "s3://$raw_bucket/" >/dev/null 2>&1; then
        log "Raw bucket verified: $raw_bucket"
    else
        log "WARNING: Raw bucket $raw_bucket not accessible, trying to discover..."
        # Quick fallback discovery with timeout
        raw_bucket=$(timeout 15 aws s3api list-buckets \
            --query 'Buckets[?contains(Name, `raw-data`)].Name' \
            --output text 2>/dev/null | head -1)
        
        if [ -z "$raw_bucket" ]; then
            log "ERROR: No raw bucket found"
            raw_bucket="aqua-hive-raw-data-82zp4fuh"  # fallback to expected name
        fi
    fi
    
    # Export bucket names for use by pipeline scripts
    export CLIMATE_RAW_BUCKET="$raw_bucket"
    export CLIMATE_TILES_BUCKET="$tiles_bucket"
    
    log "Using buckets - Raw: $raw_bucket, Tiles: $tiles_bucket"
}

# Extract MBTiles to PBF tiles and sync to S3
extract_and_sync_tiles() {
    log "Starting MBTiles extraction and S3 sync..."
    
    # Check if we have tiles bucket configured
    if [ -z "${CLIMATE_TILES_BUCKET:-}" ]; then
        log "WARNING: No tiles bucket configured, skipping tile extraction"
        return 0
    fi
    
    # Process each parameter
    for param in "${script_params[@]}"; do
        local mbtiles_dir="${param}_mbtiles_output"
        
        if [ -d "$mbtiles_dir" ]; then
            log "Processing $param MBTiles for extraction..."
            
            # Find all MBTiles files for this parameter
            local mbtiles_files=$(find "$mbtiles_dir" -name "*.mbtiles" 2>/dev/null)
            
            if [ ! -z "$mbtiles_files" ]; then
                # Create extraction directory
                local extract_dir="/tmp/${param}_tiles_extract"
                mkdir -p "$extract_dir"
                
                for mbtiles_file in $mbtiles_files; do
                    log "Extracting tiles from: $mbtiles_file"
                    
                    # Extract filename to get year/month info
                    local filename=$(basename "$mbtiles_file" .mbtiles)
                    local year_month=""
                    
                    # Try to extract year and month from filename
                    # Expected formats: precipitation_05_2025.mbtiles, temperature_2025_05.mbtiles, etc.
                    if [[ "$filename" =~ ([0-9]{4}).*([0-9]{2}) ]]; then
                        local year="${BASH_REMATCH[1]}"
                        local month="${BASH_REMATCH[2]}"
                        year_month="${year}/${month}"
                    elif [[ "$filename" =~ ([0-9]{2}).*([0-9]{4}) ]]; then
                        local month="${BASH_REMATCH[1]}"
                        local year="${BASH_REMATCH[2]}"
                        year_month="${year}/${month}"
                    else
                        # Fallback to target year/month
                        year_month="${target_year}/$(printf "%02d" $target_month)"
                    fi
                    
                    # Create parameter/year/month directory structure
                    local tile_output_dir="$extract_dir/$param/$year_month"
                    mkdir -p "$tile_output_dir"
                    
                    # Extract MBTiles to directory structure using mb-util or custom extractor
                    log "Extracting $mbtiles_file to $tile_output_dir"
                    
                    # Try mb-util first
                    if command -v mb-util >/dev/null 2>&1; then
                        if mb-util "$mbtiles_file" "$tile_output_dir" --image_format=pbf 2>/dev/null; then
                            log "Successfully extracted $mbtiles_file using mb-util"
                        else
                            log "mb-util failed, trying custom extractor..."
                            # Use our custom extractor
                            if [ -f "/usr/local/bin/extract-mbtiles" ]; then
                                python3 /usr/local/bin/extract-mbtiles "$mbtiles_file" "$tile_output_dir" || log "Custom extractor failed for $mbtiles_file"
                            else
                                log "No tile extractor available for $mbtiles_file"
                                continue
                            fi
                        fi
                    else
                        # Use custom extractor if mb-util not available
                        if [ -f "/usr/local/bin/extract-mbtiles" ]; then
                            log "Using custom MBTiles extractor..."
                            python3 /usr/local/bin/extract-mbtiles "$mbtiles_file" "$tile_output_dir" || log "Custom extractor failed for $mbtiles_file"
                        else
                            log "No tile extractor available for $mbtiles_file"
                            continue
                        fi
                    fi
                    
                    # Rename .mvt files to .pbf if they exist
                    log "Renaming .mvt files to .pbf format..."
                    find "$tile_output_dir" -name "*.mvt" -exec rename 's/\.mvt$/.pbf/' {} \; 2>/dev/null || true
                    
                    # Also handle case where files have no extension
                    find "$tile_output_dir" -type f ! -name "*.pbf" ! -name "*.json" ! -name "*.txt" -exec mv {} {}.pbf \; 2>/dev/null || true
                done
                
                # Sync extracted tiles to S3 in the proper structure: {param}/{year}/{month}/{z}/{x}/{y}.pbf
                if [ -d "$extract_dir" ] && [ "$(ls -A $extract_dir 2>/dev/null)" ]; then
                    log "Syncing extracted tiles to S3: s3://${CLIMATE_TILES_BUCKET}/"
                    
                    # Sync the entire parameter directory structure
                    aws s3 sync "$extract_dir/" "s3://${CLIMATE_TILES_BUCKET}/" \
                        --exclude "*.json" \
                        --exclude "*.txt" \
                        --include "*.pbf" \
                        --content-type "application/x-protobuf" \
                        --metadata-directive REPLACE || log "Warning: Failed to sync tiles to S3"
                    
                    log "Tile sync completed for $param"
                    
                    # Clean up extraction directory
                    rm -rf "$extract_dir"
                else
                    log "No tiles extracted for $param"
                fi
            else
                log "No MBTiles files found in $mbtiles_dir"
            fi
        else
            log "MBTiles directory $mbtiles_dir not found for $param"
        fi
    done
    
    log "MBTiles extraction and S3 sync completed!"
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
    
    # Calculate the previous month and year for processing
    local current_month=$(date +%m)
    local current_year=$(date +%Y)
    local target_month
    local target_year
    
    # Remove leading zero from month if present
    current_month=$((10#$current_month))
    
    if [ "$current_month" -eq 1 ]; then
        # If current month is January, target is December of previous year
        target_month=12
        target_year=$((current_year - 1))
    else
        # Otherwise, target is previous month of current year
        target_month=$((current_month - 1))
        target_year=$current_year
    fi
    
    log "Current date: $current_year-$(printf "%02d" $current_month)"
    log "Target processing date: $target_year-$(printf "%02d" $target_month)"
    
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
            log "Executing pipeline script: $pipeline_script for $target_year-$(printf "%02d" $target_month)"
            
            # Make script executable and run it with calculated date parameters
            chmod +x "$pipeline_script" 2>/dev/null || true
            
            # Execute script with year and month arguments
            if python3 "$pipeline_script" --start-year "$target_year" --start-month "$target_month" --end-year "$target_year" --end-month "$target_month" 2>&1 | tee -a "$LOG_FILE"; then
                log "Successfully processed $param data for $target_year-$(printf "%02d" $target_month)"
                
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
    
    log "All pipeline processing completed successfully!"
    
    # Extract MBTiles to PBF tiles and sync to S3
    extract_and_sync_tiles
    
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
