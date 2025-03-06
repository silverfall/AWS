#!/bin/bash

# Get the current date in UTC format (YYYY-MM-DD)
TODAY=$(date -u +%Y-%m-%d)

# Define the cutoff time (10 PM GMT+7 → 3 PM UTC)
CUTOFF_TIME="15:00:00"

# Fetch available EBS volumes and filter by today's date & time
VOLUMES=$(aws ec2 describe-volumes --filters "Name=status,Values=available" \
    --query "Volumes[*].[VolumeId,CreateTime]" --output text | awk -v today="$TODAY" -v cutoff="$CUTOFF_TIME" '
    $2 ~ today && $3 >= cutoff {print $1}')

# Check if any volumes were found
if [[ -z "$VOLUMES" ]]; then
    echo "No available EBS volumes found since 10 PM GMT+7."
    exit 0
fi

echo "The following EBS volumes will be deleted (Created after 10 PM GMT+7):"
echo "$VOLUMES"

# Confirmation before deletion
read -p "Are you sure? (y/N): " CONFIRM
if [[ "$CONFIRM" == "y" || "$CONFIRM" == "Y" ]]; then
    for VOLUME_ID in $VOLUMES; do
        echo "Deleting volume: $VOLUME_ID"
        aws ec2 delete-volume --volume-id "$VOLUME_ID"
    done
else
    echo "Deletion canceled."
fi
