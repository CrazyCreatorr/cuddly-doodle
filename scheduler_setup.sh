#!/bin/bash
# Full scheduler setup script to be hosted on GitHub
# This script handles creating VPC, subnets, security groups and setting up the climate data scheduler

# Usage: scheduler_setup.sh [REGION]
# Example: scheduler_setup.sh ap-south-1

set -euo pipefail

# Configuration
LOG_FILE="/var/log/scheduler-setup.log"
REGION="${1:-ap-south-1}"  # Use first argument as region or default to ap-south-1
SCRIPTS_DIR="/opt/climate-scheduler"

# Logging functions
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
error_exit() { log "ERROR: $1"; exit 1; }

# Install required packages
install_packages() {
    log "Installing required packages..."
    apt-get update -y || error_exit "Failed to update system"
    apt-get install -y python3 python3-pip git awscli jq curl || error_exit "Failed to install packages"
    pip3 install boto3 pytz || error_exit "Failed to install Python packages"
}

# Create VPC if needed
get_vpc_id() {
    log "Discovering VPC by tags..."
    
    # Try to find VPC by tags
    for tag_pattern in "aqua-hive-vpc" "climate-vpc" "iitm-vpc"; do
        vpc_id=$(aws ec2 describe-vpcs \
            --region "$REGION" \
            --filters "Name=tag:Name,Values=$tag_pattern" \
            --query 'Vpcs[0].VpcId' \
            --output text 2>/dev/null)
        
        if [ "$vpc_id" != "None" ] && [ -n "$vpc_id" ]; then
            log "Found VPC: $vpc_id"
            echo "$vpc_id"
            return 0
        fi
    done
    
    log "No matching VPC found by tags, trying to find default VPC..."
    # Try default VPC
    vpc_id=$(aws ec2 describe-vpcs \
        --region "$REGION" \
        --filters "Name=isDefault,Values=true" \
        --query 'Vpcs[0].VpcId' \
        --output text 2>/dev/null)
    
    if [ "$vpc_id" != "None" ] && [ -n "$vpc_id" ]; then
        log "Using default VPC: $vpc_id"
        echo "$vpc_id"
        return 0
    fi
    
    log "No default VPC found, trying to find any available VPC..."
    # Try any VPC
    vpc_id=$(aws ec2 describe-vpcs \
        --region "$REGION" \
        --query 'Vpcs[0].VpcId' \
        --output text 2>/dev/null)
    
    if [ "$vpc_id" != "None" ] && [ -n "$vpc_id" ]; then
        log "Using first available VPC: $vpc_id"
        echo "$vpc_id"
        return 0
    fi
    
    # Create new VPC if none found
    log "No VPC found, creating new VPC..."
    
    # Extra error handling and retries
    local max_retries=3
    local retry_count=0
    local success=false
    
    while [ $retry_count -lt $max_retries ] && [ "$success" != "true" ]; do
        vpc_id=$(aws ec2 create-vpc \
            --region "$REGION" \
            --cidr-block "10.0.0.0/16" \
            --query 'Vpc.VpcId' \
            --output text 2>/dev/null) || true
        
        if [ -n "$vpc_id" ] && [ "$vpc_id" != "None" ]; then
            success=true
            log "Successfully created VPC: $vpc_id"
        else
            retry_count=$((retry_count + 1))
            log "VPC creation failed, retry $retry_count of $max_retries..."
            sleep 5
        fi
    done
    
    if [ "$success" != "true" ]; then
        log "ERROR: Failed to create VPC after $max_retries attempts"
        exit 1
    fi
    
    # Tag the VPC
    aws ec2 create-tags \
        --region "$REGION" \
        --resources "$vpc_id" \
        --tags Key=Name,Value="aqua-hive-vpc-auto" Key=Project,Value="aqua-hive" || \
        log "Warning: Failed to tag VPC, continuing anyway"
    
    # Enable DNS hostnames
    aws ec2 modify-vpc-attribute \
        --region "$REGION" \
        --vpc-id "$vpc_id" \
        --enable-dns-hostnames "{\"Value\":true}" || \
        log "Warning: Failed to enable DNS hostnames, continuing anyway"
    
    # Create IGW with retries
    local igw_id=""
    retry_count=0
    success=false
    
    while [ $retry_count -lt $max_retries ] && [ "$success" != "true" ]; do
        igw_id=$(aws ec2 create-internet-gateway \
            --region "$REGION" \
            --query 'InternetGateway.InternetGatewayId' \
            --output text 2>/dev/null) || true
        
        if [ -n "$igw_id" ] && [ "$igw_id" != "None" ]; then
            success=true
            log "Created internet gateway: $igw_id"
        else
            retry_count=$((retry_count + 1))
            log "Internet gateway creation failed, retry $retry_count of $max_retries..."
            sleep 5
        fi
    done
    
    if [ "$success" != "true" ]; then
        log "ERROR: Failed to create internet gateway after $max_retries attempts"
        # Don't exit, try to continue without IGW
        echo "$vpc_id"
        return 0
    fi
    
    # Attach IGW to VPC
    aws ec2 attach-internet-gateway \
        --region "$REGION" \
        --internet-gateway-id "$igw_id" \
        --vpc-id "$vpc_id" || \
        log "Warning: Failed to attach internet gateway to VPC, continuing anyway"
    
    # Tag IGW
    aws ec2 create-tags \
        --region "$REGION" \
        --resources "$igw_id" \
        --tags Key=Name,Value="aqua-hive-vpc-igw" Key=Project,Value="aqua-hive" || \
        log "Warning: Failed to tag internet gateway, continuing anyway"
    
    log "Created VPC infrastructure: $vpc_id with IGW: $igw_id"
    echo "$vpc_id"
    return 0
}

# Create subnet if needed
get_subnet_id() {
    local vpc_id="$1"
    log "Discovering subnet in VPC $vpc_id..."
    
    # Try to find by tags
    for tag_pattern in "aqua-hive-public-subnet" "climate-public"; do
        subnet_id=$(aws ec2 describe-subnets \
            --region "$REGION" \
            --filters "Name=vpc-id,Values=$vpc_id" "Name=tag:Name,Values=$tag_pattern" \
            --query 'Subnets[0].SubnetId' \
            --output text 2>/dev/null)
        
        if [ "$subnet_id" != "None" ] && [ -n "$subnet_id" ]; then
            log "Found subnet: $subnet_id"
            echo "$subnet_id"
            return 0
        fi
    done
    
    log "No subnet found by tags, trying public subnet by attribute..."
    # Try public subnet by attribute
    subnet_id=$(aws ec2 describe-subnets \
        --region "$REGION" \
        --filters "Name=vpc-id,Values=$vpc_id" "Name=map-public-ip-on-launch,Values=true" \
        --query 'Subnets[0].SubnetId' \
        --output text 2>/dev/null)
    
    if [ "$subnet_id" != "None" ] && [ -n "$subnet_id" ]; then
        log "Found public subnet: $subnet_id"
        echo "$subnet_id"
        return 0
    fi
    
    log "No public subnet found, trying any subnet..."
    # Try any subnet
    subnet_id=$(aws ec2 describe-subnets \
        --region "$REGION" \
        --filters "Name=vpc-id,Values=$vpc_id" \
        --query 'Subnets[0].SubnetId' \
        --output text 2>/dev/null)
    
    if [ "$subnet_id" != "None" ] && [ -n "$subnet_id" ]; then
        log "Using first available subnet: $subnet_id"
        echo "$subnet_id"
        return 0
    fi
    
    # Create new subnet if none found
    log "No subnet found, creating new public subnet..."
    
    # Try multiple availability zones in case one is constrained
    local az_suffixes=("a" "b" "c")
    local max_retries=3
    local subnet_id=""
    local success=false
    
    # Loop through AZs and try to create subnet
    for suffix in "${az_suffixes[@]}"; do
        local az="${REGION}${suffix}"
        log "Attempting to create subnet in availability zone $az..."
        
        local retry_count=0
        while [ $retry_count -lt $max_retries ] && [ "$success" != "true" ]; do
            subnet_id=$(aws ec2 create-subnet \
                --region "$REGION" \
                --vpc-id "$vpc_id" \
                --cidr-block "10.0.1.0/24" \
                --availability-zone "$az" \
                --query 'Subnet.SubnetId' \
                --output text 2>/dev/null) || true
            
            if [ -n "$subnet_id" ] && [ "$subnet_id" != "None" ]; then
                success=true
                log "Successfully created subnet: $subnet_id in AZ: $az"
                break
            else
                retry_count=$((retry_count + 1))
                log "Subnet creation failed in AZ $az, retry $retry_count of $max_retries..."
                sleep 5
            fi
        done
        
        # Break out of AZ loop if subnet creation succeeded
        if [ "$success" = "true" ]; then
            break
        fi
    done
    
    if [ "$success" != "true" ]; then
        log "ERROR: Failed to create subnet in any availability zone"
        exit 1
    fi
    
    # Configure subnet
    aws ec2 modify-subnet-attribute \
        --region "$REGION" \
        --subnet-id "$subnet_id" \
        --map-public-ip-on-launch || \
        log "Warning: Failed to set map-public-ip-on-launch, continuing anyway"
    
    # Tag subnet
    aws ec2 create-tags \
        --region "$REGION" \
        --resources "$subnet_id" \
        --tags Key=Name,Value="aqua-hive-public-subnet" Key=Project,Value="aqua-hive" || \
        log "Warning: Failed to tag subnet, continuing anyway"
    
    # Setup routing
    # Find the IGW for this VPC
    igw_id=$(aws ec2 describe-internet-gateways \
        --region "$REGION" \
        --filters "Name=attachment.vpc-id,Values=$vpc_id" \
        --query 'InternetGateways[0].InternetGatewayId' \
        --output text 2>/dev/null)
    
    if [ -n "$igw_id" ] && [ "$igw_id" != "None" ]; then
        log "Found internet gateway: $igw_id, setting up route table..."
        
        # Create route table
        rtb_id=$(aws ec2 create-route-table \
            --region "$REGION" \
            --vpc-id "$vpc_id" \
            --query 'RouteTable.RouteTableId' \
            --output text 2>/dev/null) || true
        
        if [ -n "$rtb_id" ] && [ "$rtb_id" != "None" ]; then
            # Add route to IGW
            aws ec2 create-route \
                --region "$REGION" \
                --route-table-id "$rtb_id" \
                --destination-cidr-block "0.0.0.0/0" \
                --gateway-id "$igw_id" || \
                log "Warning: Failed to create internet route, continuing anyway"
            
            # Associate route table with subnet
            aws ec2 associate-route-table \
                --region "$REGION" \
                --subnet-id "$subnet_id" \
                --route-table-id "$rtb_id" || \
                log "Warning: Failed to associate route table, continuing anyway"
            
            # Tag route table
            aws ec2 create-tags \
                --region "$REGION" \
                --resources "$rtb_id" \
                --tags Key=Name,Value="aqua-hive-route-table" Key=Project,Value="aqua-hive" || \
                log "Warning: Failed to tag route table, continuing anyway"
            
            log "Route table setup complete: $rtb_id"
        else
            log "Warning: Failed to create route table, subnet may not have internet access"
        fi
    else
        log "Warning: No internet gateway found, subnet will not have internet access"
    fi
    
    log "Created subnet infrastructure: $subnet_id"
    echo "$subnet_id"
    return 0
}

# Get security group ID
get_security_group_id() {
    local vpc_id="$1"
    log "Discovering security group in VPC $vpc_id..."
    
    # Try by tag
    for tag_pattern in "aqua-hive-processor-sg" "climate-processor"; do
        sg_id=$(aws ec2 describe-security-groups \
            --region "$REGION" \
            --filters "Name=vpc-id,Values=$vpc_id" "Name=tag:Name,Values=$tag_pattern" \
            --query 'SecurityGroups[0].GroupId' \
            --output text 2>/dev/null)
        
        if [ "$sg_id" != "None" ] && [ -n "$sg_id" ]; then
            log "Found security group: $sg_id"
            echo "$sg_id"
            return 0
        fi
    done
    
    log "No tagged security group found, checking default security group..."
    # Try default security group
    sg_id=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=vpc-id,Values=$vpc_id" "Name=group-name,Values=default" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null)
    
    if [ "$sg_id" != "None" ] && [ -n "$sg_id" ]; then
        log "Using default security group: $sg_id"
        echo "$sg_id"
        return 0
    fi
    
    # No security group found, create one
    log "No security group found, creating new security group..."
    
    # Create security group with retries
    local max_retries=3
    local retry_count=0
    local success=false
    local sg_id=""
    
    while [ $retry_count -lt $max_retries ] && [ "$success" != "true" ]; do
        sg_id=$(aws ec2 create-security-group \
            --region "$REGION" \
            --vpc-id "$vpc_id" \
            --group-name "aqua-hive-processor-sg-auto" \
            --description "Auto-created security group for climate data processors" \
            --query 'GroupId' \
            --output text 2>/dev/null) || true
        
        if [ -n "$sg_id" ] && [ "$sg_id" != "None" ]; then
            success=true
            log "Successfully created security group: $sg_id"
        else
            retry_count=$((retry_count + 1))
            log "Security group creation failed, retry $retry_count of $max_retries..."
            sleep 5
        fi
    done
    
    if [ "$success" != "true" ]; then
        log "ERROR: Failed to create security group after $max_retries attempts"
        exit 1
    fi
    
    # Tag the security group
    aws ec2 create-tags \
        --region "$REGION" \
        --resources "$sg_id" \
        --tags Key=Name,Value="aqua-hive-processor-sg-auto" Key=Project,Value="aqua-hive" || \
        log "Warning: Failed to tag security group, continuing anyway"
    
    # Add ingress rules - SSH from anywhere (for management)
    aws ec2 authorize-security-group-ingress \
        --region "$REGION" \
        --group-id "$sg_id" \
        --protocol tcp \
        --port 22 \
        --cidr "0.0.0.0/0" || \
        log "Warning: Failed to add SSH ingress rule, continuing anyway"
    
    # Add egress rule - Allow all outbound
    aws ec2 authorize-security-group-egress \
        --region "$REGION" \
        --group-id "$sg_id" \
        --protocol all \
        --port -1 \
        --cidr "0.0.0.0/0" || \
        log "Warning: Failed to add outbound rule, continuing anyway"
    
    log "Created security group with basic rules: $sg_id"
    echo "$sg_id"
    return 0
}

# Get launch template ID
get_launch_template_id() {
    log "Discovering launch template..."
    
    # Try by tag
    for tag in "aqua-hive" "climate-data" "processor"; do
        template_id=$(aws ec2 describe-launch-templates \
            --region "$REGION" \
            --filters "Name=tag:Name,Values=*$tag*" \
            --query 'LaunchTemplates[0].LaunchTemplateId' \
            --output text 2>/dev/null)
        
        if [ "$template_id" != "None" ] && [ -n "$template_id" ]; then
            log "Found template by tag: $template_id"
            echo "$template_id"
            return 0
        fi
    done
    
    log "No launch template found by tags, trying by name..."
    # Try by name
    for name in "aqua-hive" "climate" "processor"; do
        template_id=$(aws ec2 describe-launch-templates \
            --region "$REGION" \
            --filters "Name=launch-template-name,Values=*$name*" \
            --query 'LaunchTemplates[0].LaunchTemplateId' \
            --output text 2>/dev/null)
        
        if [ "$template_id" != "None" ] && [ -n "$template_id" ]; then
            log "Found template by name: $template_id"
            echo "$template_id"
            return 0
        fi
    done
    
    log "No launch template found by name, trying most recent template..."
    # Most recent template
    template_id=$(aws ec2 describe-launch-templates \
        --region "$REGION" \
        --query 'sort_by(LaunchTemplates, &CreateTime)[-1].LaunchTemplateId' \
        --output text 2>/dev/null)
    
    if [ "$template_id" != "None" ] && [ -n "$template_id" ]; then
        log "Using most recent template: $template_id"
        echo "$template_id"
        return 0
    fi
    
    log "ERROR: No launch template found. Cannot proceed without a launch template."
    log "Please create a launch template for processor instances first."
    log "The scheduler will continue running but no processor instances can be created."
    
    # Return empty template ID - the spot_manager will need to handle this case
    echo "MISSING_TEMPLATE"
    return 1
}

# Setup spot manager
setup_spot_manager() {
    log "Setting up spot instance manager..."
    
    # Check if spot_manager.py exists in the cloned repository
    if [ -f "$SCRIPTS_DIR/spot_manager.py" ]; then
        log "Found spot_manager.py in repository"
        cp "$SCRIPTS_DIR/spot_manager.py" /opt/spot_manager.py
        chmod +x /opt/spot_manager.py
    else
        # If not in repository, create from scratch with minimal functionality
        log "spot_manager.py not found in repository, creating minimal version..."
        cat > /opt/spot_manager.py << 'EOF'
#!/usr/bin/env python3
"""
Spot Instance Manager for Climate Data Processing
This script launches EC2 spot instances based on the provided launch template.
"""

import argparse
import boto3
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Launch EC2 spot instances for climate data processing')
    
    parser.add_argument('--region', required=True, help='AWS region')
    parser.add_argument('--vpc-id', required=True, help='VPC ID')
    parser.add_argument('--subnet-id', required=True, help='Subnet ID')
    parser.add_argument('--security-group-id', required=True, help='Security group ID')
    parser.add_argument('--launch-template-id', required=True, help='Launch template ID')
    parser.add_argument('--instance-count', type=int, default=1, help='Number of instances to launch')
    
    return parser.parse_args()

def launch_spot_instance(ec2_client, args):
    """Launch spot instance using the specified template."""
    try:
        # Get the current month and year for tagging
        current_date = datetime.now()
        month_year = f"{current_date.month:02d}-{current_date.year}"
        
        # Prepare the launch specification
        launch_spec = {
            'LaunchTemplateId': args.launch_template_id,
            'NetworkInterfaces': [{
                'DeviceIndex': 0,
                'SubnetId': args.subnet_id,
                'Groups': [args.security_group_id],
                'AssociatePublicIpAddress': True
            }]
        }
        
        # Request spot instance
        logger.info(f"Requesting spot instance using template {args.launch_template_id}")
        response = ec2_client.request_spot_instances(
            InstanceCount=args.instance_count,
            LaunchSpecification=launch_spec,
            TagSpecifications=[
                {
                    'ResourceType': 'spot-instances-request',
                    'Tags': [
                        {'Key': 'Name', 'Value': f'climate-processor-{month_year}'},
                        {'Key': 'Project', 'Value': 'aqua-hive'},
                        {'Key': 'ProcessingMonth', 'Value': month_year}
                    ]
                }
            ]
        )
        
        # Get spot request ID
        spot_request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
        logger.info(f"Spot request created: {spot_request_id}")
        
        # Wait for the spot request to be fulfilled
        logger.info("Waiting for spot request to be fulfilled...")
        waiter = ec2_client.get_waiter('spot_instance_request_fulfilled')
        waiter.wait(
            SpotInstanceRequestIds=[spot_request_id],
            WaiterConfig={'Delay': 5, 'MaxAttempts': 12}  # 1 minute timeout
        )
        
        # Get the instance ID
        response = ec2_client.describe_spot_instance_requests(
            SpotInstanceRequestIds=[spot_request_id]
        )
        instance_id = response['SpotInstanceRequests'][0].get('InstanceId')
        
        if instance_id:
            logger.info(f"Spot instance launched: {instance_id}")
            
            # Tag the instance
            ec2_client.create_tags(
                Resources=[instance_id],
                Tags=[
                    {'Key': 'Name', 'Value': f'climate-processor-{month_year}'},
                    {'Key': 'Project', 'Value': 'aqua-hive'},
                    {'Key': 'ProcessingMonth', 'Value': month_year}
                ]
            )
            return instance_id
        else:
            logger.error("Failed to get instance ID from spot request")
            return None
    
    except Exception as e:
        logger.error(f"Error launching spot instance: {str(e)}")
        return None

def main():
    """Main function."""
    args = parse_args()
    
    # Check if launch template exists
    if args.launch_template_id == "MISSING_TEMPLATE":
        logger.error("No launch template provided. Cannot launch spot instances.")
        return 1
    
    # Initialize boto3 client
    try:
        ec2_client = boto3.client('ec2', region_name=args.region)
    except Exception as e:
        logger.error(f"Failed to initialize EC2 client: {str(e)}")
        return 1
    
    # Launch spot instance
    instance_id = launch_spot_instance(ec2_client, args)
    
    if instance_id:
        logger.info(f"Successfully launched spot instance {instance_id}")
        return 0
    else:
        logger.error("Failed to launch spot instance")
        return 1

if __name__ == "__main__":
    sys.exit(main())
EOF
        chmod +x /opt/spot_manager.py
    fi
    
    log "Spot manager setup completed"
}

# Setup cron job for automated scheduling
setup_cron() {
    local vpc_id="$1"
    local sg_id="$2"
    local subnet_id="$3"
    local template_id="$4"
    
    log "Setting up monthly cron job..."
    
    # Create the cron job file
    cat > /etc/cron.d/climate-processing << EOT
# Monthly climate data processing - runs on the 2nd day of each month at 2 AM
0 2 2 * * root /usr/bin/python3 /opt/spot_manager.py --region ${REGION} --vpc-id ${vpc_id} --security-group-id ${sg_id} --subnet-id ${subnet_id} --launch-template-id ${template_id} >> ${LOG_FILE} 2>&1
EOT
    
    log "Cron job setup completed"
}

# Main execution
main() {
    log "=== Climate Scheduler Setup Started ==="
    
    # Install packages
    install_packages
    
    # Get network resources
    vpc_id=$(get_vpc_id)
    if [ -z "$vpc_id" ]; then
        log "ERROR: Failed to get or create VPC. Exiting."
        exit 1
    fi
    log "Using VPC: $vpc_id"
    
    subnet_id=$(get_subnet_id "$vpc_id")
    if [ -z "$subnet_id" ]; then
        log "ERROR: Failed to get or create subnet. Exiting."
        exit 1
    fi
    log "Using subnet: $subnet_id"
    
    security_group_id=$(get_security_group_id "$vpc_id")
    if [ -z "$security_group_id" ]; then
        log "ERROR: Failed to get or create security group. Exiting."
        exit 1
    fi
    log "Using security group: $security_group_id"
    
    launch_template_id=$(get_launch_template_id)
    if [ "$launch_template_id" = "MISSING_TEMPLATE" ]; then
        log "WARNING: No launch template found. Spot instances will not be created."
        log "Continuing with scheduler setup without running spot manager."
    else
        log "Using launch template: $launch_template_id"
        
        # Setup spot manager
        setup_spot_manager
        
        # Run spot manager only if we have a valid template
        log "Running spot manager..."
        python3 /opt/spot_manager.py \
            --region "$REGION" \
            --vpc-id "$vpc_id" \
            --security-group-id "$security_group_id" \
            --subnet-id "$subnet_id" \
            --launch-template-id "$launch_template_id" || \
            log "Warning: Failed to run spot manager, will retry via cron"
    fi
    
    # Setup cron job regardless of whether spot manager ran successfully
    if [ "$launch_template_id" != "MISSING_TEMPLATE" ]; then
        setup_cron "$vpc_id" "$security_group_id" "$subnet_id" "$launch_template_id"
        log "Cron job has been set up for monthly processing"
    else
        log "Skipping cron job setup due to missing launch template"
    fi
    
    log "=== Climate Scheduler Setup Completed ==="
}

# Run main function
main 2>&1 | tee -a "$LOG_FILE"
