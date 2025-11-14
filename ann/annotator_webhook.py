# annotator_webhook.py
#
# Modified to run as a web server that can be called by SNS to process jobs
# Run using: python annotator_webhook.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "annotator_webhook_config.Config"
app.config.from_object(environment)



# Load config
app.config.from_object("annotator_webhook_config.Config")
REGION = app.config["AWS_REGION"]
SQS_QUEUE_URL = app.config["SQS_QUEUE_URL"]
S3_INPUT_BUCKET = app.config["S3_INPUTS_BUCKET"]
S3_RESULTS_BUCKET = app.config["S3_RESULTS_BUCKET"]
DDB_TABLE = app.config["DDB_TABLE"]
LOCAL_JOB_DIR = app.config["LOCAL_JOB_DIR"]

# AWS clients
sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(DDB_TABLE)

# Connect to SQS and get the message queue


@app.route("/", methods=["GET"])
def annotator_webhook():

    return ("Annotator webhook; POST job to /process-job-request"), 200


@app.route("/", methods=["GET"])
def home():
    return "Annotator Webhook is running", 200

"""
A13 - Replace polling with webhook in annotator

Receives request from SNS; queries job queue and processes message.
Reads request messages from SQS and runs AnnTools as a subprocess.
Updates the annotations database with the status of the request.
"""


@app.route("/process-job-request", methods=["POST"])
def annotate():

    # Check message type

    # Confirm SNS topic subscription

    # Process job request

    try:
        sns_message_type = request.headers.get("x-amz-sns-message-type")

        # --------------------------
        # 1. SUBSCRIPTION CONFIRMATION
        # --------------------------
        if sns_message_type == "SubscriptionConfirmation":
            data = request.get_json()
            token = data["Token"]
            topic_arn = data["TopicArn"]

            sns = boto3.client("sns", region_name=REGION)
            sns.confirm_subscription(TopicArn=topic_arn, Token=token)

            return jsonify({"status": "Subscription confirmed"}), 200

        # --------------------------
        # 2. SNS NOTIFICATION — PROCESS JOB
        # --------------------------
        if sns_message_type == "Notification":

            # Read ONE message from SQS
            sqs_resp = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1
            )

            if "Messages" not in sqs_resp:
                return jsonify({"message": "No SQS messages found"}), 200

            msg = sqs_resp["Messages"][0]
            receipt = msg["ReceiptHandle"]
            body = json.loads(msg["Body"])

            # SNS wraps SQS; unwrap if needed
            if "Message" in body:
                job = json.loads(body["Message"].replace("'", '"'))
            else:
                job = body

            print("[INFO] Job received:", job)

            job_id = job["job_id"]
            user_id = job["user_id"]
            key = job["s3_key_input_file"]

            # --------------------------
            # Download input file
            # --------------------------
            local_user_dir = os.path.join(LOCAL_JOB_DIR, user_id, job_id)
            os.makedirs(local_user_dir, exist_ok=True)

            filename = key.split("/")[-1]
            local_path = os.path.join(local_user_dir, filename)

            s3.download_file(S3_INPUT_BUCKET, key, local_path)

            # Set status to RUNNING
            table.update_item(
                Key={"user_id": user_id, "job_id": job_id},
                UpdateExpression="SET job_status = :s",
                ExpressionAttributeValues={":s": "RUNNING"},
            )

            # --------------------------
            # Launch AnnTools
            # --------------------------
            subprocess.Popen(
                ["python3", "run.py", local_path, job_id, user_id]
            )

            # Delete SQS message
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt
            )

            return jsonify({"message": "Job accepted"}), 202

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    return jsonify({"message": "Ignored"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

### EOF





