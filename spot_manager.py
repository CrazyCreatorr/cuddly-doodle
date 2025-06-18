#!/usr/bin/env python3
"""
Dynamic Spot Instance Manager for Climate Data Processing
Self-contained script with dynamic resource discovery
"""
import boto3
import argparse
import time
import json
import logging
from datetime import datetime, timedelta
import pytz

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SpotInstanceManager:
    def __init__(self, region, vpc_id, security_group_id, subnet_id, launch_template_id):
        self.region = region
        self.vpc_id = vpc_id
        self.security_group_id = security_group_id
        self.subnet_id = subnet_id
        self.launch_template_id = launch_template_id
        
        self.ec2_client = boto3.client('ec2', region_name=region)
        self.ec2_resource = boto3.resource('ec2', region_name=region)
        
        logger.info(f"Initialized SpotInstanceManager for region: {region}")
        logger.info(f"VPC: {vpc_id}, Security Group: {security_group_id}, Subnet: {subnet_id}")
        logger.info(f"Launch Template: {launch_template_id}")
    
    def request_spot_instance(self, instance_type='t3.large', max_price='0.50'):
        """Request a spot instance for processing using launch template with fallback instance types"""
        # List of instance types to try in order of preference
        instance_types = [
            't3.large',     # Most commonly available, good for general workloads
            't3.xlarge',    # More power if needed
            'm5.large',     # Alternative general purpose
            'm5.xlarge',    # More memory
            'c5.large',     # Compute optimized, smaller
            'c5.xlarge',    # Compute optimized, medium
            't2.large'      # Last resort, older generation
        ]
        
        # Start with the requested instance type if not in our list
        if instance_type not in instance_types:
            instance_types.insert(0, instance_type)
        
        try:
            # Calculate current month for processing (process previous month's data)
            now = datetime.now()
            if now.month == 1:
                target_year = now.year - 1
                target_month = 12
            else:
                target_year = now.year
                target_month = now.month - 1
            
            logger.info(f"Requesting spot instance to process data for {target_year}-{target_month:02d}")
            logger.info(f"Using launch template: {self.launch_template_id}")
            
            # Try each instance type until one succeeds
            last_error = None
            for i, try_instance_type in enumerate(instance_types):
                try:
                    logger.info(f"Attempting to launch {try_instance_type} (attempt {i+1}/{len(instance_types)})")
                    
                    # Use run_instances with market options instead of request_spot_instances
                    # This allows us to use the launch template which contains our user_data_processor.sh
                    # Override the subnet and security group to ensure they match our current VPC
                    response = self.ec2_client.run_instances(
                        MaxCount=1,
                        MinCount=1,
                        InstanceType=try_instance_type,  # Override instance type
                        LaunchTemplate={
                            'LaunchTemplateId': self.launch_template_id,
                            'Version': '$Latest'
                        },
                        SubnetId=self.subnet_id,  # Override subnet from discovery
                        SecurityGroupIds=[self.security_group_id],  # Override security group from discovery
                        InstanceMarketOptions={
                            'MarketType': 'spot',
                            'SpotOptions': {
                                'MaxPrice': max_price,
                                'SpotInstanceType': 'one-time',
                                'InstanceInterruptionBehavior': 'terminate'
                            }
                        },
                        TagSpecifications=[
                            {
                                'ResourceType': 'instance',
                                'Tags': [
                                    {'Key': 'Name', 'Value': 'aqua-hive-processor-spot'},
                                    {'Key': 'Project', 'Value': 'aqua-hive'},
                                    {'Key': 'Type', 'Value': 'processor'},
                                    {'Key': 'ProcessingYear', 'Value': str(target_year)},
                                    {'Key': 'ProcessingMonth', 'Value': str(target_month)},
                                    {'Key': 'InstanceType', 'Value': try_instance_type}
                                ]
                            }
                        ]
                    )
                    
                    instance_id = response['Instances'][0]['InstanceId']
                    logger.info(f"Spot instance launched successfully: {instance_id} ({try_instance_type})")
                    
                    return instance_id
                    
                except Exception as e:
                    last_error = e
                    if "InsufficientInstanceCapacity" in str(e) or "SpotMaxPriceTooLow" in str(e):
                        logger.warning(f"No capacity for {try_instance_type}: {e}")
                        if i < len(instance_types) - 1:
                            logger.info(f"Trying next instance type...")
                            continue
                    else:
                        # For other errors, don't retry with different instance types
                        logger.error(f"Error launching {try_instance_type}: {e}")
                        raise
            
            # If we get here, all instance types failed
            raise Exception(f"Failed to launch spot instance with any instance type. Last error: {last_error}")
            
        except Exception as e:
            logger.error(f"Error requesting spot instance: {e}")
            raise
    
    def check_instance_status(self, instance_id):
        """Check the status of an instance"""
        try:
            response = self.ec2_client.describe_instances(
                InstanceIds=[instance_id]
            )
            
            if response['Reservations'] and response['Reservations'][0]['Instances']:
                instance = response['Reservations'][0]['Instances'][0]
                state = instance['State']['Name']
                logger.info(f"Instance {instance_id} state: {state}")
                
                if state == 'running':
                    return 'running', instance_id
                elif state in ['pending']:
                    return 'pending', None
                else:
                    return state, None
            
            return 'unknown', None
            
        except Exception as e:
            logger.error(f"Error checking spot request status: {e}")
            return 'error', None
    
    def schedule_monthly_processing(self):
        """Schedule monthly processing by requesting spot instances"""
        logger.info("Starting monthly processing scheduler")
        
        try:
            # Request spot instance for processing using launch template
            instance_id = self.request_spot_instance()
            
            # Monitor the instance
            max_wait_time = 300  # 5 minutes
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                status, _ = self.check_instance_status(instance_id)
                
                if status == 'running':
                    logger.info(f"Processing started on instance: {instance_id}")
                    break
                elif status == 'pending':
                    logger.info("Waiting for spot instance to start...")
                    time.sleep(30)
                else:
                    logger.error(f"Instance failed with status: {status}")
                    break
            
            logger.info("Monthly processing scheduling completed")
            
        except Exception as e:
            logger.error(f"Error in monthly processing: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Spot Instance Manager for Climate Data Processing')
    parser.add_argument('--region', required=True, help='AWS region')
    parser.add_argument('--vpc-id', required=True, help='VPC ID')
    parser.add_argument('--security-group-id', required=True, help='Security Group ID')
    parser.add_argument('--subnet-id', required=True, help='Subnet ID')
    parser.add_argument('--launch-template-id', required=True, help='Launch Template ID')
    
    args = parser.parse_args()
    
    manager = SpotInstanceManager(
        args.region,
        args.vpc_id,
        args.security_group_id,
        args.subnet_id,
        args.launch_template_id
    )
    
    manager.schedule_monthly_processing()

if __name__ == "__main__":
    main()
