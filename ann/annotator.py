import subprocess
import boto3
import json
from botocore.exceptions import ClientError
import os

# Get util configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

region_name = config["ann"]['AwsRegionName']
queue_name = config["ann"]['AwsQueueName']
WaitTimeBetweenMessage = int(config["ann"]["WaitTimeBetweenMessage"])
dynamo_table_name = config["ann"]["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

sqs_client = boto3.client("sqs", region_name = region_name)


def receive_message(url):
    try:
        response = sqs_client.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=WaitTimeBetweenMessage,
        )
    except ClientError as e:
        print(e)
        return None
    messages = response.get("Messages", [])
    print(f"Number of messages received: {len(messages)}")
    if len(messages) > 0:
        return messages[0]
    return None

def parse_response(response):
    try:
        json_response = json.loads(response)
        json_body = json.loads(json_response["Message"])
        return json_body
    except Exception as e:
        print(e)
        return None

def delete_msg(url, handle):
    try:
        response = sqs_client.delete_message(
            QueueUrl=url,
            ReceiptHandle=handle,
        )
        messageID = response["ResponseMetadata"]["RequestId"]
        print("message deleted")
    except ClientError as e:
        print(e)

try:
    queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
except ClientError as e:
    print(e)

while True:
    # Long poll for message on provided SQS queue, if no response, continue
    response = receive_message(queue_url)
    if response == None:
        continue
    

    message_handle = response["ReceiptHandle"]
    message_body = response["Body"]

    # If json_data is not successfully parsed, continue
    json_data = parse_response(message_body)
    if json_data == None:
        continue

    job_id = json_data["job_id"]
    user_id = json_data["user_id"]
    input_file_name = json_data["input_file_name"]
    s3_inputs_bucket = json_data["s3_inputs_bucket"]
    s3_key_input_file = json_data["s3_key_input_file"]
    submit_time = json_data["submit_time"]
    json_data["job_status"] = "RUNNING"

    # Get the input file S3 object and copy it to a local file
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_inputs_bucket)
    try:
        s3.meta.client.head_bucket(Bucket=s3_inputs_bucket)
    except ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(e)
            continue

    filename = s3_key_input_file.split("/")[2]
    f = open("annotation-files/"+filename, "w")
    f.close()

    # Try to download file from bucket
    try:
        bucket.download_file(s3_key_input_file, 
                            'annotation-files/' + filename)
        print("Annotation file succssfully retrieved from bucket")
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        print(e)
        continue

    # Run the annotator using hw5_run
    """try:
        process = subprocess.Popen(["python", "run.py", "annotation-files/" + filename], stdout=subprocess.PIPE)
        print("A new process was spawned to run the annotation file")
    except subprocess.CalledProcessError as e:
        print(e)
        continue"""
    
    # Update dynamoDB to show process as running
    try:
        dynamo = boto3.resource('dynamodb')
        table = dynamo.Table(dynamo_table_name)
        response = table.update_item(
            Key = {"job_id": job_id}, 
            UpdateExpression="set job_status = :j",
            ConditionExpression = "job_status = :p",
            ExpressionAttributeValues={":j": "RUNNING", ":p": "PENDING"},
            ReturnValues="UPDATED_OLD"
        )
        print("Dyanomo DB updated job status to PENDING")

    # The dynamo DB won't be updated to RUNNING, but since the job is already
    # running, the message should still be deleted
    except ClientError as e:
        print(e)
        pass

    # Delete the message using its handle
    delete_msg(queue_url, message_handle)
