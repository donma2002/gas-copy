# References:
# [1] AWS Boto3 S3 – “Example 1: Upload a file to an S3 bucket”
#     https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#example-1-upload-a-file-to-an-s3-bucket
# [2] AWS DynamoDB UpdateItem API Reference – Example: “Setting, Removing, and Updating Attributes in an Item”
#     https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_Examples
# [3] Python glob – “Example – find all .py files in directory”
#     https://docs.python.org/3/library/glob.html#example
# [4] Python shutil.copy() – Copying files and directories
#     https://docs.python.org/3/library/shutil.html#shutil.copy
# [5] Python sys.argv – Command-line arguments example
#     https://docs.python.org/3/library/sys.html#sys.argv
# [6] Real Python – “Using time.time() for timestamps and elapsed time”
#     https://realpython.com/python-time-module/#getting-the-current-time
# [7] StackOverflow – “Uploading files to S3 using boto3 with error handling”
#     https://stackoverflow.com/questions/33842944/uploading-files-to-s3-using-boto3
# [8] StackOverflow – “How to find the latest file in a folder using Python glob”
#     https://stackoverflow.com/questions/6773584/how-to-find-the-latest-file-in-a-folder-using-python
# [9] Python os and os.path – “Using os.path.abspath() and os.remove()”
#     https://docs.python.org/3/library/os.html#os.remove
# [10] Python uuid – “Generate a random UUID”
#     https://docs.python.org/3/library/uuid.html#uuid.uuid4

import sys       # [5]
import time      # [6]
import os        # [9]
import glob      # [3][8]
import shutil    # [4]
import boto3     # [1][2][7]
import uuid      # [10]

sys.path.append(os.path.dirname(os.path.abspath(__file__)))  # [9]
import driver

# Configuration constants
CNETID = "donma"
USERNAME = "userX"
REGION = "us-east-1"
S3_RESULTS_BUCKET = "gas-results"

# Initialize AWS clients
s3 = boto3.client("s3", region_name=REGION)                # [1][7]
dynamodb = boto3.resource("dynamodb", region_name=REGION)  # [2]
table = dynamodb.Table(f"{CNETID}_annotations")


def upload_to_s3(local_path, bucket, key):
    """
    Uploads a local file to an S3 bucket.
    References:
      [1] Boto3 example – upload_file() usage.
      [7] StackOverflow – upload_file() with error handling.
    """
    try:
        s3.upload_file(local_path, bucket, key)  # [1][7]
        print(f"Uploaded {local_path} to s3://{bucket}/{key}")
    except Exception as e:
        print(f"ERROR uploading {local_path} to s3://{bucket}/{key}: {e}")
        raise


def update_job_status(job_id, result_key, log_key):
    """
    Updates the DynamoDB record for a job to COMPLETED.
    References:
      [2] AWS UpdateItem example – using UpdateExpression and ExpressionAttributeValues.
      [6] Real Python – getting Unix timestamps with time.time().
    """
    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=(
                "SET s3_results_bucket = :b, "
                "s3_key_result_file = :rf, "
                "s3_key_log_file = :lf, "
                "complete_time = :t, "
                "job_status = :s"
            ),
            ExpressionAttributeValues={
                ":b": S3_RESULTS_BUCKET,
                ":rf": result_key,
                ":lf": log_key,
                ":t": int(time.time()),  # [6]
                ":s": "COMPLETED"
            }
        )
        print(f"Job {job_id} updated to COMPLETED in DynamoDB")
    except Exception as e:
        print(f"ERROR updating DynamoDB for job {job_id}: {e}")


if __name__ == "__main__":
    """
    Main entry point for the AnnTools annotation runner.
    References:
      [5] Using sys.argv for command-line parameters.
      [4][8][9] Using shutil, glob, and os for file management and cleanup.
    """
    if len(sys.argv) < 3:    # [5]
        print("Usage: python3 a8_run.py <input_vcf_file> <job_id>")
        sys.exit(1)

    input_file_name = sys.argv[1]
    job_id = sys.argv[2]
    job_dir = os.path.dirname(os.path.abspath(input_file_name))  # [9]

    print(f"Starting AnnTools job for job_id: {job_id}")
    print(f"Input file: {input_file_name}")

    start_time = time.time()   # [6]

    try:
        # Run annotation process
        driver.run(input_file_name, "vcf")
        print(f"Annotation completed in {time.time() - start_time:.2f}s")

        # Locate intermediate output files
        print("Locating result files...")
        temp_files = sorted(glob.glob(input_file_name + ".*"))  # [3][8]
        if not temp_files:
            print(f"ERROR: Could not locate intermediate output for {input_file_name}")
            sys.exit(1)

        # Copy latest intermediate file as final annotated output
        final_tmp = temp_files[-1]
        annotated_path = input_file_name + ".annotated"
        shutil.copy(final_tmp, annotated_path)  # [4]

        # Write job log file
        log_path = input_file_name + ".log"
        with open(log_path, "w") as logf:
            logf.write(f"Annotation job {job_id} completed successfully.\n")
            logf.write(f"Generated from {os.path.basename(final_tmp)}\n")

        print(f"Created {annotated_path} and {log_path}")

        # Define S3 object keys
        s3_prefix = f"{CNETID}/{job_id}/"
        result_key = s3_prefix + os.path.basename(annotated_path)
        log_key = s3_prefix + os.path.basename(log_path)

        # Upload results to S3
        upload_to_s3(annotated_path, S3_RESULTS_BUCKET, result_key)  # [1][7]
        upload_to_s3(log_path, S3_RESULTS_BUCKET, log_key)           # [1][7]

        # Update DynamoDB status
        update_job_status(job_id, result_key, log_key)               # [2][6]

        # Cleanup temporary and input files
        for path in [input_file_name, annotated_path, log_path] + temp_files:
            try:
                os.remove(path)  # [9]
                print(f"Deleted local file: {path}")
            except Exception as e:
                print(f"Warning: could not delete {path}: {e}")

        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"ERROR during annotation job {job_id}: {e}")
        sys.exit(1)
