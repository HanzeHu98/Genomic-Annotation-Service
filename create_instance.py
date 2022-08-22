import boto3

ec2 = boto3.resource('ec2')
cnet_id = "hanzeh"
ami = 'ami-0627971cd8c777781'

instances = ec2.create_instances(
    MinCount=1,
    MaxCount=1,
    ImageId=ami,
    InstanceType="t2.nano",
    IamInstanceProfile={'Name': 'instance_profile_' + cnet_id},
    TagSpecifications=[{
    'ResourceType': 'instance',
    'Tags': [{'Key': 'Name', 'Value': cnet_id}]
    }],
    KeyName=cnet_id,
    SecurityGroups=['mpcs-cc']
    )

instance = instances[0]
print(instance.id)
print(instance.public_dns_name)