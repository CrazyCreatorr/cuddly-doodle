#!/bin/bash
# Monitor script to check the status of the climate processing system

set -euo pipefail

REGION="${1:-ap-south-1}"

echo "=== Climate Processing System Monitor ==="
echo "Region: $REGION"
echo "Time: $(date)"
echo

# Check scheduler instance
echo "--- Scheduler Instance ---"
scheduler_info=$(aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Type,Values=scheduler" "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].{InstanceId:InstanceId,State:State.Name,PublicIp:PublicIpAddress}' \
    --output table 2>/dev/null)

if [ ! -z "$scheduler_info" ]; then
    echo "$scheduler_info"
else
    echo "No running scheduler instance found"
fi
echo

# Check processor instances
echo "--- Processor Instances ---"
processor_info=$(aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Type,Values=processor" \
    --query 'Reservations[*].Instances[*].{InstanceId:InstanceId,State:State.Name,LaunchTime:LaunchTime,ProcessingStatus:Tags[?Key==`ProcessingStatus`].Value|[0],ProcessingMonth:Tags[?Key==`ProcessingMonth`].Value|[0],ProcessingYear:Tags[?Key==`ProcessingYear`].Value|[0]}' \
    --output table 2>/dev/null)

if [ ! -z "$processor_info" ]; then
    echo "$processor_info"
else
    echo "No processor instances found"
fi
echo

# Check recent spot instance requests
echo "--- Recent Spot Instance Requests (last 24 hours) ---"
spot_requests=$(aws ec2 describe-spot-instance-requests \
    --region "$REGION" \
    --filters "Name=create-time,Values=$(date -d '24 hours ago' -u +%Y-%m-%d)" \
    --query 'SpotInstanceRequests[*].{RequestId:SpotInstanceRequestId,State:State,Status:Status.Code,InstanceId:InstanceId,CreateTime:CreateTime}' \
    --output table 2>/dev/null)

if [ ! -z "$spot_requests" ]; then
    echo "$spot_requests"
else
    echo "No recent spot instance requests found"
fi
echo

# Check S3 buckets
echo "--- S3 Buckets ---"
buckets=$(aws s3api list-buckets \
    --query 'Buckets[?contains(Name, `aqua-hive`) || contains(Name, `climate`)].{Name:Name,CreationDate:CreationDate}' \
    --output table 2>/dev/null)

if [ ! -z "$buckets" ]; then
    echo "$buckets"
else
    echo "No relevant S3 buckets found"
fi
echo

echo "=== End of Monitor Report ==="
echo "To SSH into scheduler: ssh -i ~/.ssh/YOUR_KEY.pem ubuntu@SCHEDULER_IP"
echo "To check cron job: sudo cat /etc/cron.d/climate-processing"
echo "To check logs: sudo tail -f /var/log/scheduler-setup.log"
