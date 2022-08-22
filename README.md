# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

[Event Handler](https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html)
[Converting time integer to string](https://docs.python.org/3/library/time.html)
[lambda function sqs events](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html)
[config files](https://stackoverflow.com/questions/8884188/how-to-read-and-write-ini-file-with-python3)
### Restore
[Archive retrieval job initiation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job)
[Restoreing Archived Jobs](https://stackoverflow.com/questions/64755906/restore-glacier-object-to-s3-standard-tier)
[AWS Initiate Job](https://docs.aws.amazon.com/amazonglacier/latest/dev/api-initiate-job-post.html)
### Archive
[Uploading to Glacier](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive)
[Deleting from S3](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object)
### Ann
[Importing package](https://blog.finxter.com/python-how-to-import-modules-from-another-folder/#:~:text=The%20most%20Pythonic%20way%20to,import%20module%20.)
### view.py
[Presigned Download](https://stackoverflow.com/questions/60163289/how-do-i-create-a-presigned-url-to-download-a-file-from-an-s3-bucket-using-boto3)
[Presigned URL](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html)
[Reading file from S3 Bucket](https://www.slsmk.com/use-boto3-to-open-an-aws-s3-file-directly/)
[Condition Expression in Query](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax)
