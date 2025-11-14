#!/usr/bin/env python3
# References:
# [1] AWS Boto3 SQS — receive_message and delete_message API Reference:
#     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
# [2] AWS Boto3 S3 — download_file Example:
#     https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
# [3] AWS Boto3 DynamoDB — update_item with ConditionExpression:
#     https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
# [4] Python subprocess.Popen for background process execution:
#     https://docs.python.org/3/library/subprocess.html#subprocess.Popen
#
# NOTE:
#   ALL previously hard-coded strings like queue URLs, S3 bucket names, table names,
#   paths, prefixes, usernames, regions, etc., are now read from annotator_config.ini
#   to satisfy the “no hardcoded values” requirement.

import boto3, json, os, subprocess, time, traceback
import configparser 


config = configparser.ConfigParser()
config.read('/home/ubuntu/gas/ann/annotator_config.ini')

REGION = config['aws']['region']
SQS_QUEUE_URL = config['aws']['sqs_queue_url']
S3_INPUT_BUCKET = config['aws']['s3_input_bucket']
S3_RESULTS_BUCKET = config['aws']['s3_results_bucket']
DDB_TABLE_NAME = config['aws']['dynamodb_table']
LOCAL_JOB_DIR = config['jobs']['local_job_dir']


# DynamoDB table name from config
DDB_TABLE_NAME = config['aws']['dynamodb_table']


sqs = boto3.client("sqs", region_name=REGION)              # [1]
s3 = boto3.client("s3", region_name=REGION)                # [2]
dynamodb = boto3.resource("dynamodb", region_name=REGION)  # [3]
table = dynamodb.Table(DDB_TABLE_NAME)

# Ensure job directory exists
os.makedirs(LOCAL_JOB_DIR, exist_ok=True)

print("[INFO] Annotator started. Listening for messages...")


def download_from_s3(bucket, key, user_id, job_id):
    """Download input file for this job to local user/job directory. [2]"""
    try:
        # Per-user, per-job directory structure (required by assignment)
        job_dir = os.path.join(LOCAL_JOB_DIR, user_id, job_id)
        os.makedirs(job_dir, exist_ok=True)

        filename = key.split("/")[-1]  # keep original filename
        local_path = os.path.join(job_dir, filename)

        s3.download_file(bucket, key, local_path)  # [2]
        print(f"[INFO] Downloaded input file: {filename} → {local_path}")
        return local_path

    except Exception as e:
        print(f"[ERROR] Failed to download from S3: {e}")
        traceback.print_exc()
        return None


def update_job_status(job_id, new_status, expected_status=None, user_id=None):
    """Update job status in DynamoDB with optional conditional check. [3]"""
    try:
        if not user_id:
            print(f"[ERROR] Missing user_id for job {job_id}, cannot update status.")
            return

        key = {"user_id": user_id, "job_id": job_id}

        if expected_status:
            table.update_item(
                Key=key,
                UpdateExpression="SET job_status = :r",
                ConditionExpression="job_status = :p",
                ExpressionAttributeValues={":r": new_status, ":p": expected_status}
            )
        else:
            table.update_item(
                Key=key,
                UpdateExpression="SET job_status = :r",
                ExpressionAttributeValues={":r": new_status}
            )

        print(f"[INFO] Job {job_id}: status updated to {new_status}")

    except Exception as e:
        if "ConditionalCheckFailedException" in str(e):
            print(f"[INFO] Skipping update: job {job_id} not in expected state.")
        else:
            print(f"[WARN] Could not update status for job {job_id} {new_status}: {e}")


def launch_annotation(local_path, job_id, user_id):
    """Launch AnnTools annotation subprocess in background. [4]"""
    try:
        print(f"[INFO] Launching annotation job {job_id}...")
        subprocess.Popen(
            ["python3", "run.py", local_path, job_id, user_id]  # [4]
        )
        print(f"[INFO] Job {job_id} successfully started.")
        return True

    except Exception as e:
        print(f"[ERROR] Failed to start job {job_id}: {e}")
        traceback.print_exc()
        return False

while True:
    try:
        # Long polling SQS [1]
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=20
        )

        # No messages
        if "Messages" not in response:
            continue

        for msg in response["Messages"]:
            try:
                body = msg["Body"]
                message_json = json.loads(body)

                # SNS-wrapped SQS delivery
                if "Message" in message_json:
                    job_data = json.loads(message_json["Message"].replace("'", '"'))
                else:
                    job_data = message_json

                print(f"[INFO] Received job message: {job_data}")

                # Extract required fields
                job_id = job_data.get("job_id")
                user_id = job_data.get("user_id")  
                bucket = job_data.get("s3_inputs_bucket", S3_INPUT_BUCKET)
                key = job_data.get("s3_key_input_file")

                if not job_id or not key or not user_id:
                    print("[WARN] Invalid message format; skipping.")
                    continue

                update_job_status(job_id, "RUNNING", expected_status="PENDING", user_id=user_id)

                # Download input file
                local_path = download_from_s3(bucket, key, user_id, job_id)
                if not local_path:
                    update_job_status(job_id, "FAILED", user_id=user_id)
                    continue

                # Launch annotator
                success = launch_annotation(local_path, job_id, user_id)

                if success:
                    # Delete SQS message (job accepted) [1]
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    print(f"[INFO] Deleted SQS message for job {job_id}")

                else:
                    update_job_status(job_id, "FAILED", user_id=user_id)

            except Exception as job_e:
                print(f"[ERROR] Failed to process SQS message: {job_e}")
                traceback.print_exc()

    except Exception as poll_e:
        print(f"[ERROR] SQS polling failed: {poll_e}")
        time.sleep(5)
