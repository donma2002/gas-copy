# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
# [1] AWS Boto3 DynamoDB — put_item API Reference:
#     https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
# [2] AWS Boto3 SNS — publish API Reference:
#     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html

__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for

from app import app, db
from decorators import authenticated, is_premium
from decimal import Decimal

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
def create_annotation_job_request():

    region = app.config["AWS_REGION_NAME"]

    # Parse redirect URL query parameters for S3 object info
    bucket_name = request.args.get("bucket")
    s3_key = request.args.get("key")

    if not bucket_name or not s3_key:
        app.logger.error("Missing bucket or key in upload redirect.")
        return abort(400)

    # Extract job info
    user_id = session["primary_identity"]
    filename = s3_key.split("~")[-1]
    job_id = str(uuid.uuid4())
    submit_time = int(time.time())

    # Insert job record into DynamoDB [1]
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    item = {
        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": filename,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": submit_time,
        "job_status": "PENDING",
    }

    try:
        table.put_item(Item=item)
        app.logger.info(f"Job {job_id} added to DynamoDB.")
    except Exception as e:
        app.logger.error(f"Unable to insert job {job_id} into DynamoDB: {e}")
        return abort(500)

    # Publish message to SNS for downstream processing [2]
    sns = boto3.client("sns", region_name=region)
    message = json.dumps(item)

    try:
        sns.publish(
            TopicArn=app.config["AWS_SNS_JOB_REQUEST_TOPIC"],
            Message=message,
        )
        app.logger.info(f"Published SNS job request for {job_id}.")
    except Exception as e:
        app.logger.error(f"Failed to publish SNS message for {job_id}: {e}")
        return abort(500)

    # Render confirmation page
    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    """
    Lists all annotation jobs for the currently logged-in user.
    Fetches job metadata from DynamoDB and formats timestamps
    for display in the 'annotations.html' template.
    """

    region = app.config["AWS_REGION_NAME"]
    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    user_id = session["primary_identity"]

    # Connect to DynamoDB
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    try:
        # Query for all jobs belonging to this user
        response = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        items = response.get("Items", [])
    except Exception as e:
        app.logger.error(f"Unable to query DynamoDB for user {user_id}: {e}")
        return abort(500)

    items.sort(key=lambda x: x.get("submit_time", 0), reverse=True)

    for item in items:
        if "submit_time" in item:
            ts = item["submit_time"]
            if isinstance(ts, Decimal):
                ts = float(ts)
            item["request_time_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        else:
            item["request_time_str"] = "N/A"


        if "complete_time" in item:
            ts = item["complete_time"]
            if isinstance(ts, Decimal):
                ts = float(ts) 
            item["complete_time_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        else:
            item["complete_time_str"] = "N/A"

    return render_template("annotations.html", annotations=items)


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    user_id = session["primary_identity"]
    dynamodb = boto3.resource("dynamodb", region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    try:
        response = table.get_item(Key={"user_id": user_id, "job_id": id})
        item = response.get("Item", None)
    except Exception as e:
        app.logger.error(f"Error getting job {id} from DynamoDB: {e}")
        abort(500)


    if item is None:
        abort(404)
    if item["user_id"] != user_id:
        abort(403)

    # Convert epoch times
    if "submit_time" in item:
        ts = item["submit_time"]
        if isinstance(ts, Decimal):
            ts = float(ts)
        item["request_time_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    else:
        item["request_time_str"] = "N/A"

    if "complete_time" in item:
        ts = item["complete_time"]
        if isinstance(ts, Decimal):
            ts = float(ts)
        item["complete_time_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    else:
        item["complete_time_str"] = "N/A"

    # Generate pre-signed download URLs for input & results files
    s3 = boto3.client("s3", region_name=app.config["AWS_REGION_NAME"], config=Config(signature_version="s3v4"))
    bucket_name = app.config["AWS_S3_RESULTS_BUCKET"]
    input_bucket = app.config["AWS_S3_INPUTS_BUCKET"]

    input_file_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": input_bucket, "Key": item["s3_key_input_file"]},
        ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"]
    )

    item["input_file_url"] = input_file_url

    if item["job_status"] == "COMPLETED":
        result_file_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": item["s3_key_result_file"]},
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"]
        )
        item["result_file_url"] = result_file_url

    return render_template("annotation.html", annotation=item)

"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    user_id = session["primary_identity"]
    dynamodb = boto3.resource("dynamodb", region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    try:
        response = table.get_item(Key={"user_id": user_id, "job_id": id})
        item = response.get("Item", None)
    except Exception as e:
        app.logger.error(f"Error getting job {id}: {e}")
        abort(500)

    if item is None:
        abort(404)
    if item["user_id"] != user_id:
        abort(403)

    # Fetch the log file from S3
    s3 = boto3.client("s3", region_name=app.config["AWS_REGION_NAME"])
    bucket_name = app.config["AWS_S3_RESULTS_BUCKET"]
    log_key = item["s3_key_log_file"]

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=log_key)
        log_data = obj["Body"].read().decode("utf-8")
    except Exception as e:
        app.logger.error(f"Unable to read log file {log_key}: {e}")
        abort(500)

    return render_template("view_log.html", log_content=log_data, job_id=id)



"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info

        # If A15 not completed, force-upgrade user role and initiate restoration
        pass

    elif request.method == "POST":
        # Process the subscription request

        # Create a customer on Stripe

        # Subscribe customer to pricing plan

        # Update user role in accounts database

        # Update role in the session

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # ...and make sure you handle files pending archive!

        # Display confirmation page
        pass


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
