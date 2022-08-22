# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID and file name from the S3 key
  split_key = s3_key.split("~")
  filename = split_key[1]
  split_directory = split_key[0].split("/")
  job_id = split_directory[2]

  # Extract the user id
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)

  # Get DynamoDB table name from config
  dynamo_table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  region_name = app.config['AWS_REGION_NAME']

  # Create a job item and persist it to the annotations database
  data = { "job_id": job_id,
           "user_id": user_id,
           "input_file_name": filename,
           "s3_inputs_bucket": bucket_name,
           "s3_key_input_file": s3_key,
           "submit_time": int(time.time()),
           "job_status": "PENDING"
          }
  try:
    dynamo = boto3.resource('dynamodb', region_name=region_name)
    table = dynamo.Table(dynamo_table_name)
    response = table.put_item(Item = data)
  except ClientError as e:
    app.logger.info(f"An Error was encountered when pushing to dyanmoDB: {e}")
    return abort(500)

  # Get SNS Topic ARN from config
  sns_topic_arn = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]

  # Send message to request queue
  try:
    sns = boto3.client("sns", region_name = region_name)
    sns.publish(
      TopicArn=sns_topic_arn,
      MessageStructure="json",
      Message=json.dumps({'default': json.dumps(data)})
    )
  except ClientError as e:
    app.logger.info(f"An Error was encountered when publishing to JOB Request SNS: {e}")
    return abort(500)

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  # Get variables from config
  bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
  region_name = app.config["AWS_REGION_NAME"]
  aws_prefix = app.config["AWS_S3_KEY_PREFIX"]
  dynamo_table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

  # Get User Id
  user_id = session['primary_identity']

  annotations = []
  try:
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(dynamo_table_name)
    response = table.query(
      IndexName = "user_id_index",
      KeyConditionExpression = Key('user_id').eq(user_id)
    )
    annotations = response['Items']
  except ClientError as e:
    app.logger.info(f"An Error was encountered when querying dynamoDB: {e}")
    return abort(500)

  for annotation in annotations:
    annotation["submit_time"] = convert_int_to_time(annotation["submit_time"])

  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Get variables from config
  region_name = app.config["AWS_REGION_NAME"]
  aws_prefix = app.config["AWS_S3_KEY_PREFIX"]
  dynamo_table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

  # Get User Id
  user_id = session['primary_identity']
  profile = get_profile(identity_id=session.get('primary_identity'))
  annotation = {}
  try:
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(dynamo_table_name)
    response = table.get_item(Key = {'job_id': id })
    annotation = response["Item"]
  except ClientError as e:
    app.logger.info(f"An Error was encountered when querying dynamoDB: {e}")
    return abort(500)

  if len(annotation) == 0:
    app.logger.info(f"No annotation with id {id} was found")
    return abort(404)
  
  if annotation["user_id"] != user_id:
    app.logger.info(f"Trying to view annotation that does not belong to you!")
    return abort(403)

  try:
    input_file_name = annotation["input_file_name"]
    input_file_path = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    annotation["job_id"] + f'~{input_file_name}'
    s3 = boto3.client("s3")
    signed_download_input_file = s3.generate_presigned_url(
      'get_object',
      Params={
        'Bucket': app.config["AWS_S3_INPUTS_BUCKET"],
        'Key': input_file_path
      },
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
    )
    annotation["input_file_url"] = signed_download_input_file
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned download for input file: {e}")
    return abort(500)

  annotation["submit_time"] = convert_int_to_time(annotation["submit_time"])
  free_access_expired = False
  if "complete_time" in annotation:
    time_remaining = 300 - (int(time.time()) - annotation["complete_time"])
    annotation["complete_time"] = convert_int_to_time(annotation["complete_time"])
    annotation_file_name = annotation["s3_key_result_file"]
    app.logger.info(time_remaining)
    if profile.role == "premium_user":
      time_remaining = 300
    if time_remaining > 0:
      try:
        s3 = boto3.client("s3")
        presigned_download = s3.generate_presigned_url(
          'get_object',
          Params={
            'Bucket': app.config["AWS_S3_RESULTS_BUCKET"],
            'Key': annotation_file_name
          },
          ExpiresIn=time_remaining
        )
        annotation["result_file_url"] = presigned_download
      except ClientError as e:
        app.logger.error(f"Unable to generate generate presigned download for result file: {e}")
        return abort(500)
    else:
      free_access_expired = True

  return render_template('annotation_details.html', annotation=annotation, free_access_expired = free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):# Get variables from config
  region_name = app.config["AWS_REGION_NAME"]
  s3_result_bucket = app.config["AWS_S3_RESULTS_BUCKET"]

  annotation = {}
  try:
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
    response = table.get_item(Key = {'job_id': id })
    annotation = response["Item"]
  except ClientError as e:
    app.logger.info(f"An Error was encountered when querying dynamoDB: {e}")
    return abort(500)

  # Get the input file S3 object and copy it to a local file
  s3 = boto3.client("s3", region_name = region_name)

  # Try to download file from bucket
  file_data = ""
  try:
    file_to_read = annotation["s3_key_log_file"]
    #Create a file object using the bucket and object key. 
    fileobj = s3.get_object(
      Bucket=s3_result_bucket,
      Key=file_to_read
    ) 

    # open the file object and read it into the variable filedata. 
    file_data = fileobj['Body'].read().decode("utf-8") 
    app.logger.info("Annotation file content retrieved from bucket")
  except ClientError as e:
    app.logger.info(f"Unable to read log data file: {e}")
    abort(500)

  return render_template('view_log.html', job_id=id, log_file_contents = file_data)

"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"
    user_id = session['primary_identity']
    dynamo_table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # Get all annotations that has been archived for this user
    annotations = []
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(dynamo_table_name)

    try:
      response = table.query(
        IndexName = "user_id_index",
        KeyConditionExpression = Key('user_id').eq(user_id),
        FilterExpression = "attribute_exists (results_file_archive_id)"
      )
      annotations = response['Items']
      app.logger.info(annotations)
    except ClientError as e:
      app.logger.info(f"An Error was encountered when querying dynamoDB: {e}")
      return abort(500)
    
    region_name = app.config["AWS_REGION_NAME"]
    sns_restore_topic = app.config["AWS_SNS_RESULT_RESTORE_TOPIC"]

    # Send message to Result Restore queue
    for annotation in annotations:
      try:
        job_id = annotation["job_id"]
        data = {
          "results_file_archive_id": annotation["results_file_archive_id"],
          "user_id": annotation["user_id"],
          "job_id": job_id
        }
        sns = boto3.client("sns", region_name = region_name)
        sns.publish(
          TopicArn=sns_restore_topic,
          MessageStructure="json",
          Message=json.dumps({'default': json.dumps(data)})
        )
      except ClientError as e:
        app.logger.info(f"An Error was encountered when publishing to result restore SNS: {e}")
        return abort(500)
      
      try:
        response = table.update_item(
          Key = {"job_id": job_id},
          UpdateExpression="""set restore_message = :m""",
          ExpressionAttributeValues= {":m": "Your Annotation Result File is Currently being restored"},
          ReturnValues="UPDATED_OLD"
        )
        app.logger.info(response)
      except ClientError as e:
        app.logger.info(f"An Error was encountered when Updating restore message: {e}")
        return abort(500)

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))

"""
## ------------------------- HELPER FUNCTIONS -------------------------------##
"""
def convert_int_to_time(decimal_time):
  """
  Convert an epoch time in integer format to a string in 
  DD MMM YYYY HH:MM:SS format
  """
  return time.strftime("%d %b %Y %H:%M:%S", time.localtime(decimal_time))

"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF