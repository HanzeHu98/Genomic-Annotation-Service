# restore.py
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

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here

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
sqs_queue_name = config["aws"]['AwsSqsRestore']
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
    results_file_archive_id = json_data["results_file_archive_id"]
    job_id = json_data["job_id"]

    # Initiate Job to restore archived data
    client = boto3.client('glacier', region_name = config["aws"]["AwsRegionName"])
    glacier_vault = config["aws"]["AwsGlacierVault"]
    expedited_success = False
    try:
        response = client.initiate_job(
            vaultName = glacier_vault,
            jobParameters = {
                "Type": "archive-retrieval",
                "ArchiveId": results_file_archive_id,
                "Tier": "Expedited",
            }
        )
        expedited_success = True
        print(response)
    except ClientError as e:
        print(e)
        pass

    if not expedited_success:
        try:
            response = client.initiate_job(
                vaultName = glacier_vault,
                jobParameters = {
                    "Type": "archive-retrieval",
                    "ArchiveId": results_file_archive_id
                }
            )
            print(response)
        except ClientError as e:
            print(e)
            continue

    try:
        data = {
          "restore_request_id": response["ResponseMetadata"]["RequestId"],
          "restore_job_id": response["jobId"],
          #"s3_key_result_file" = json_data["s3_key_result_file"]
          "results_file_archive_id": results_file_archive_id,
          "user_id": user_id,
          "job_id": job_id
        }
        sns = boto3.client("sns", region_name = region_name)
        sns.publish(
          TopicArn=config["aws"]["AwsSnsThawTopic"],
          MessageStructure="json",
          Message=json.dumps({'default': json.dumps(data)})
        )
    except ClientError as e:
        print(f"An Error was encountered when publishing to result thaw SNS: {e}")

    # Delete the message using its handle
    delete_msg(queue_url, message_handle)

### EOF