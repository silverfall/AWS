import asyncio
import aioboto3
import csv
import time
from datetime import datetime

region = 'region'
MAX_ATTACHED_VOLUMES = 10
MOUNT_POINTS = [f"/mnt/data{i}" for i in range(1, MAX_ATTACHED_VOLUMES + 1)]
allocated_ebs_paths = set()

def read_csv_file(filename):
    with open(filename, newline='') as csvfile:
        return [row[0] for row in csv.reader(csvfile)]

async def execute_ssm_command(ssm_client, instance_id, command, working_dir="/"):
    response = await ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [command], "workingDirectory": [working_dir]}
    )
    command_id = response["Command"]["CommandId"]
    while True:
        output = await ssm_client.get_command_invocation(
            CommandId=command_id, InstanceId=instance_id
        )
        if output["Status"] in ["Success", "Failed", "TimedOut", "Cancelled"]:
            return output["StandardOutputContent"], output["StandardErrorContent"]
        await asyncio.sleep(5)

async def create_volume_from_snapshot(ec2_client, snapshot_id, availability_zone):
    response = await ec2_client.create_volume(
        SnapshotId=snapshot_id, AvailabilityZone=availability_zone, VolumeType='gp3'
    )
    volume_id = response['VolumeId']
    while True:
        volume_status = await ec2_client.describe_volumes(VolumeIds=[volume_id])
        if volume_status['Volumes'][0]['State'] == 'available':
            return volume_id
        await asyncio.sleep(5)

async def attach_volume(ec2_client, instance_id, snapshot_id, volume_id):
    async with asyncio.Lock():
        for letter in 'efghijklmnopqrstuvwxyz':
            ebs_path = f"/dev/sd{letter}"
            if ebs_path not in allocated_ebs_paths:
                allocated_ebs_paths.add(ebs_path)
                await ec2_client.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device=ebs_path)
                return ebs_path
        return None

async def process_snapshot(session, instance_id, snapshot_id, s3_bucket, application_name, application_prefix, availability_zone, timestamp, mount_index):
    async with session.client('ec2', region_name=region) as ec2_client,
               session.client('ssm', region_name=region) as ssm_client:
        volume_id = await create_volume_from_snapshot(ec2_client, snapshot_id, availability_zone)
        ebs_path = await attach_volume(ec2_client, instance_id, snapshot_id, volume_id)
        if not ebs_path:
            return
        mount_target = MOUNT_POINTS[mount_index % len(MOUNT_POINTS)]
        await execute_ssm_command(ssm_client, instance_id, f"sudo mount {ebs_path} {mount_target}")
        await asyncio.sleep(5)
        await execute_ssm_command(ssm_client, instance_id, f"aws s3 cp . s3://{s3_bucket}/{application_name}/{volume_id}-{timestamp}/ --recursive --exclude '*' --include '{application_prefix}-*'")
        await ec2_client.detach_volume(VolumeId=volume_id)
        await ec2_client.delete_volume(VolumeId=volume_id)
        allocated_ebs_paths.discard(ebs_path)

async def main():
    s3_bucket = 'bucket_name'
    application_name = 'apps'
    application_prefix = 'prefix' # prefix inside s3 bucket
    instance_id = 'i-0071a489e22925f88'
    availability_zone = 'your AZ'
    timestamp = datetime.now().strftime("%Y-%m-%dT%H.%M.%S")
    snapshot_ids = read_csv_file('list_snapshot.csv')
    async with aioboto3.Session() as session:
        tasks = [
            process_snapshot(session, instance_id, snapshot_id, s3_bucket, application_name, application_prefix, availability_zone, timestamp, i)
            for i, snapshot_id in enumerate(snapshot_ids)
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
