# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
import boto3
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Add utility code here
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_config.ini')

"""
## ------------------------- HELPER FUNCTIONS -------------------------------##
"""
def receive_message(url):
    try:
        response = sqs_client.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=int(config["aws"]["WaitTimeBetweenMessage"])
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
    print(response)
    try:
        json_response = json.loads(response)
        json_body = json.loads(json_response["Message"])
        return json_body
    except Exception as e:
        print("Parse Error")
        print(e)
        return None

def archive_file(result_file_key):
    s3 = boto3.client("s3", region_name = region_name)
    s3_result_bucket = config["aws"]["AwsS3ResultsBucket"]
    print(result_file_key)
    # Try to download file from bucket
    file_data = ""
    try:
        #Create a file object using the bucket and object key. 
        fileobj = s3.get_object(
            Bucket=s3_result_bucket,
            Key=result_file_key
        )
        # open the file object and read it into the variable filedata. 
        file_data = fileobj['Body'].read()
        print("Annotation file content retrieved from bucket:")
        print(file_data)
    except ClientError as e:
        print(f"Unable to read result data file: {e}")
        return None

    # Try to move data to glacier
    glacier_name = config["aws"]["AwsGlacierVault"]
    try:
        glacier_client = boto3.client('glacier')
        response = glacier_client.upload_archive(vaultName=glacier_name, body=file_data)
        print("Glacier Response:")
        print(response)
        file_archive_id = response["archiveId"]
    except ClientError as e:
        print(f"Unable to upload data file to glacier: {e}")
        return None

    # Delete file from bucket if item moved to glacier
    try:
        response = s3.delete_object(
            Bucket = s3_result_bucket,
            Key=result_file_key
        )
        print("Delete File Response:")
        print(response)
    except ClientError as e:
        print(f"Unable to delete result data file: {e}")
        return None
    
    return file_archive_id

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

"""
## ---------------------------- MAIN LOOP ------------------------------------##
"""
# Fetch item from config file
region_name = config["aws"]['AwsRegionName']
sqs_queue_name = config["aws"]['AwsSqsArchive']
dynamo_table_name = config["aws"]["AwsDynamodbAnnotationsTable"]

try:
    sqs_client = boto3.client("sqs", region_name = region_name)
    queue_url = sqs_client.get_queue_url(QueueName=sqs_queue_name)["QueueUrl"]
except ClientError as e:
    print("Error Here")
    print(e)
    raise e

# Main Loop
# Add utility code here

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
    
    # Remove file from S3 and add to Glacier
    user_id = json_data["user_id"]
    result_file_key = json_data["result_file_key"]
    job_id = json_data["job_id"]

    # Check if user is a premium user, delete message and move on
    if helpers.get_user_profile(id=user_id)[4] == "premium_user":
        delete_msg(queue_url, message_handle)
        continue

    # If user is a free user, archive the data
    file_archive_id = archive_file(result_file_key)
    if not file_archive_id:
        print(response)
        continue

    # Update dynamoDB to show process as running
    try:
        dynamo = boto3.resource('dynamodb')
        table = dynamo.Table(dynamo_table_name)
        response = table.update_item(
            Key = {"job_id": job_id}, 
            UpdateExpression="set results_file_archive_id = :j",
            ExpressionAttributeValues={":j": file_archive_id},
            ReturnValues="UPDATED_OLD"
        )
        print(f"Added file archive id {file_archive_id} to DynamoDB.")

    # The dynamo DB won't be updated to RUNNING, but since the job is already
    # running, the message should still be deleted
    except ClientError as e:
        print(e)
        continue

    # Delete the message using its handle
    delete_msg(queue_url, message_handle)

### EOF