# hw3_run.py
# Edited by Hanze Hu
#
#
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import os
import time
from botocore.exceptions import ClientError
import boto3
import json
from configparser import ConfigParser

sys.path.append("../util")
import helpers
sys.path.append("anntools")
import driver

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
            # Get util configuration
            config = ConfigParser(os.environ)
            config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))
            dynamo_table_name = config["run"]["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

            filepath = sys.argv[1]
            filename = filepath.split("/")[1]
            job_id = filename.split("~")[0]

            try:
                dynamo = boto3.resource('dynamodb')
                table = dynamo.Table(dynamo_table_name)
                response = table.get_item(Key = {'job_id': job_id })
                annotation = response["Item"]
            except ClientError as e:
                app.logger.info(f"An Error was encountered when querying dynamoDB: {e}")
            
            s3 = boto3.resource("s3")
            aws_prefix = config["run"]["AWS_S3_KEY_PREFIX"]
            bucket_name = config["run"]["AWS_S3_RESULTS_BUCKET"]

            exists = True
            bucket = s3.Bucket(bucket_name)
            try:
                s3.meta.client.head_bucket(Bucket=bucket_name)
            except ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    exists = False
                    raise e
            
            local_countlog_file = filepath+".count.log"
            local_annot_file = filepath[:-3] + "annot." + filepath[-3:]

            bucket_countlog_file = aws_prefix + annotation["user_id"] + "/" + \
                local_countlog_file.split("/")[1]
            bucket_annot_file = aws_prefix + annotation["user_id"] + "/" + \
                local_annot_file.split("/")[1]

            bucket.upload_file(local_countlog_file, bucket_countlog_file)
            bucket.upload_file(local_annot_file, bucket_annot_file)

            os.remove(local_countlog_file)
            os.remove(local_annot_file)
            os.remove(filepath)
            
            # Update dynamoDB status to complete and update complete time
            dynamo_table_name = config["run"]["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
            try:
                dynamo = boto3.resource('dynamodb')
                table = dynamo.Table(dynamo_table_name)
                response = table.update_item(
                    Key = {"job_id": job_id},
                    UpdateExpression="""set job_status = :j,
                                        complete_time = :t,
                                        s3_key_result_file = :rf,
                                        s3_key_log_file = :lf
                                        """,
                    ExpressionAttributeValues= {":j": "COMPLETED",
                                                ":t": int(time.time()),                                               
                                                ":rf": bucket_annot_file,
                                                ":lf": bucket_countlog_file
                                                },
                    ReturnValues="UPDATED_OLD"
                )
            except ClientError as e:
                print(e)
            print("Dynamo DB Updated to Completed")

            # Get SNS Topic ARN from config
            sns_topic_arn = config["run"]["AWS_SNS_JOB_COMPLETE_TOPIC"]

            # Send message to request queue
            try:
                print()
                email = {
                    "email":helpers.get_user_profile(id=annotation["user_id"])[2],
                    "job_id":job_id
                }
                sns = boto3.client("sns")
                sns.publish(
                    TopicArn=sns_topic_arn,
                    MessageStructure="json",
                    Message=json.dumps({'default': json.dumps(email)}),
                    Subject="Your GAS task has been completed"
                )
            except ClientError as e:
                print(e)
            print("Complete Topic SNS published")
            sns_topic_arn = config["run"]["AWS_SNS_ARCHIVE_TOPIC"]

            # Send message to delayed archive queue
            try:
                message = {
                    "user_id":annotation["user_id"],
                    "result_file_key": bucket_annot_file,
                    "job_id": job_id
                }
                sns = boto3.client("sns")
                sns.publish(
                    TopicArn=sns_topic_arn,
                    MessageStructure="json",
                    Message=json.dumps({'default': json.dumps(message)}),
                    Subject="Your GAS task has been completed"
                )
            except ClientError as e:
                print(e)
            print("Archive Topic SNS published")
        print("run finished running")
    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF
