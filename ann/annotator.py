import subprocess
import os
import json
import boto3
import botocore
from configparser import ConfigParser

jobs_dir = './jobs'

if not os.path.exists(jobs_dir):
    os.makedirs(jobs_dir)


# source:  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
# source: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-long-polling.html
# source: https://stackoverflow.com/questions/18973418/os-mkdirpath-returns-oserror-when-directory-does-not-exist

CONFIG_FILE = '/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini'
def process_annotations():
    sqs = boto3.client('sqs', region_name='us-east-1')
    config = ConfigParser()
    config.read_file(open(CONFIG_FILE))

    queue_name = config.get('AWS', 'SQSRequestsQueueName')
    queue_name_dlq = config.get('AWS', 'SQSRequestsDLQQueueName')

    try:
        queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        #TODO: what is urldql?
        url_dql = sqs.get_queue_url(QueueName=queue_name_dlq)['QueueUrl']
    except botocore.exceptions.ClientError as e:  # Queue Not Found
        print({
            'code': 500,
            'status': 'Server Error',
            'message': f'An error occurred: {e}'
        })

    print('... checking for messages ...')

    while True:
        try:
            #  read a message from the sqs queue
            queue = sqs.receive_message(
            QueueUrl=queue_url, AttributeNames=['All'], MaxNumberOfMessages=1,
            WaitTimeSeconds=20)
            receipt_handle = queue['Messages'][0]['ReceiptHandle']
            message_json = json.loads(queue['Messages'][0]['Body'])
            message = json.loads(message_json['Message'])
        except KeyError:
            # since there's no messages in the queue, keep listening...
            continue

        key = message['s3_key_input_file']
        user_id = message['user_id']
        uuid = message['job_id']
        filename = message['input_file_name']
        user_email = message['user_email']
        bucket_name = message['s3_inputs_bucket']

        # Save the file in a unique path
        dir_path = os.path.join(jobs_dir, user_id, uuid)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, filename)

        # Download the file from S3
        download_file_from_s3(bucket_name, key, file_path)
        print('File processed successfully with path', file_path)

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        ann_table = dynamodb.Table(config.get('AWS', 'DynamoDBTable'))
        response = ann_table.get_item(Key={'job_id': uuid})
        status = response['Item']['job_status']

        # handle if the job is already complete
        if status == 'COMPLETED':
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print("message deleted")
            print("Job is already complete!")
            continue

        try:
            subprocess.Popen(['python', 'hw5_run.py', file_path, dir_path, user_id, uuid, user_email])
        except subprocess.CalledProcessError as e:
            print({'code': 500, 'status': 'error', 'message': f'Failed to launch annotation job: {str(e)}'})

        try:
            ann_table.update_item(
                Key={'job_id': uuid},
                UpdateExpression='SET job_status = :new_status',
                ConditionExpression='job_status = :current_status',
                ExpressionAttributeValues={
                    ':new_status': 'RUNNING',
                    ':current_status': 'PENDING'
                }
            )
        except botocore.exceptions.ClientError:
            print({
                'code': 'HTTP_500_INTERNAL_SERVER_ERROR',
                'status': 'error',
                'message': 'Cannot find that key'
            })

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        print("message deleted")
        response = {
            "code": 201,
            "data": {
                "job_id": uuid,
                "input_file": filename
            },
            "status": "Success"
        }
        print(response)


# https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/get-object.html
def download_file_from_s3(bucket_name, key, file_path):
    try:
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.download_file(bucket_name, key, file_path)
    except Exception as e:
        print(f"Error downloading file from S3: {str(e)}")
        return None


if __name__ == '__main__':
    process_annotations()
