# References:
# [1] AWS Boto3 SQS — receive_message and delete_message API Reference:
#     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
# [2] AWS Boto3 S3 — download_file Example:
#     https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
# [3] AWS Boto3 DynamoDB — update_item with ConditionExpression:
#     https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
# [4] Python subprocess.Popen for background process execution:
#     https://docs.python.org/3/library/subprocess.html#subprocess.Popen

#!/usr/bin/env python3
import boto3, json, os, subprocess, time, traceback  # [1][2][3][4]

CNETID = "donma"
REGION = "us-east-1"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/127134666975/donma_job_requests"
S3_INPUT_BUCKET = "gas-inputs"
LOCAL_JOB_DIR = "/home/ubuntu/annotator_jobs"

sqs = boto3.client("sqs", region_name=REGION)              # [1]
s3 = boto3.client("s3", region_name=REGION)                # [2]
dynamodb = boto3.resource("dynamodb", region_name=REGION)  # [3]
table = dynamodb.Table(f"{CNETID}_annotations")

os.makedirs(LOCAL_JOB_DIR, exist_ok=True)

print("[INFO] A9 Annotator started. Listening for messages...")

def download_from_s3(bucket, key, job_id):
    """Download input file for this job to local storage. [2]"""
    try:
        job_dir = os.path.join(LOCAL_JOB_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)
        filename = key.split("~")[-1]
        local_path = os.path.join(job_dir, filename)
        s3.download_file(bucket, key, local_path)  # [2]
        print(f"[INFO] Downloaded {filename} to {local_path}")
        return local_path
    except Exception as e:
        print(f"[ERROR] Failed to download from S3: {e}")
        traceback.print_exc()
        return None


def update_job_status(job_id, new_status, expected_status=None):
    """Update job status in DynamoDB with optional conditional check. [3]"""
    try:
        if expected_status:
            table.update_item(  # [3]
                Key={"job_id": job_id},
                UpdateExpression="SET job_status = :r",
                ConditionExpression="job_status = :p",
                ExpressionAttributeValues={":r": new_status, ":p": expected_status}
            )
        else:
            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET job_status = :r",
                ExpressionAttributeValues={":r": new_status}
            )
        print(f"[INFO] Job {job_id}: status updated to {new_status}")
    except Exception as e:
        print(f"[WARN] Could not update status for {job_id} → {new_status}: {e}")


def launch_annotation(local_path, job_id):
    """Launch AnnTools annotation subprocess in background. [4]"""
    try:
        print(f"[INFO] Launching annotation job {job_id}...")
        subprocess.Popen(["python3", "anntools/a9_run.py", local_path, job_id])  # [4]
        print(f"[INFO] Job {job_id} successfully started.")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to start job {job_id}: {e}")
        traceback.print_exc()
        return False

while True:
    try:
        # Long poll messages from SQS [1]
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=20
        )

        if "Messages" not in response:
            continue

        for msg in response["Messages"]:
            try:
                body = msg["Body"]
                message_json = json.loads(body)
                if "Message" in message_json:  # SNS to SQS wrapping
                    job_data = json.loads(message_json["Message"].replace("'", '"'))
                else:
                    job_data = message_json

                print(f"[INFO] Received message for job: {job_data}")

                job_id = job_data.get("job_id")
                bucket = job_data.get("s3_inputs_bucket", S3_INPUT_BUCKET)
                key = job_data.get("s3_key_input_file")

                if not job_id or not key:
                    print("[WARN] Invalid message format, skipping.")
                    continue

                update_job_status(job_id, "RUNNING", expected_status="PENDING")  # [3]

                local_path = download_from_s3(bucket, key, job_id)
                if not local_path:
                    update_job_status(job_id, "FAILED")
                    continue

                success = launch_annotation(local_path, job_id)
                if success:
                    sqs.delete_message(  # [1]
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    print(f"[INFO] Deleted message for job {job_id}")
                else:
                    update_job_status(job_id, "FAILED")

            except Exception as job_e:
                print(f"[ERROR] Failed to process message: {job_e}")
                traceback.print_exc()

    except Exception as poll_e:
        print(f"[ERROR] SQS polling failed: {poll_e}")
        time.sleep(5)
