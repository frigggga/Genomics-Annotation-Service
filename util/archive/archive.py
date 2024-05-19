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
import boto3
import botocore
import json
import ast

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('archive_config.ini')


# Add utility code here

def archive_result():
    # Connect to SQS and get the archive message queue, and connect to s3
    try:
        sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        dynamodb = boto3.client('dynamodb', region_name=config['aws']['AwsRegionName'])
        s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
        glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
        queue_name = config.get('aws', 'SQSArchiveQueueName')
        queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    except botocore.exceptions.ClientError as e:  # Queue Not Found
        print({
            'code': 500,
            'status': 'Server Error',
            'message': f'Queue not found: {e}'
        })

    print('... checking for archive messages ...')

    while True:
        try:
            #  read a message from the sqs queue
            queue = sqs.receive_message(
                QueueUrl=queue_url, AttributeNames=['All'], MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            receipt_handle = queue["Messages"][0]["ReceiptHandle"]
            message_json = json.loads(queue["Messages"][0]["Body"])
            message = ast.literal_eval(message_json["Message"])
        except KeyError:
            # since there's no messages in the queue, keep listening...
            continue

        user_id = message['user_id']
        uuid = message['job_id']
        result_file_key = message['s3_key_result_file']

        profile = helpers.get_user_profile(id=user_id)

        if profile['role'] == 'premium_user':
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue
        else:
            bucket_name = config['aws']['ResultsBucket']

            # get result file from s3
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
                result_file = s3.get_object(Bucket=bucket_name, Key=result_file_key)
                content = result_file['Body'].read()
            except botocore.exceptions.ClientError as e:
                print(e)

            # Archive the result file from s3 to glacier
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive

                vault_name = config['aws']['VaultName']
                response = glacier.upload_archive(vaultName=vault_name, body=content)
                archive_id = response['ResponseMetadata']['HTTPHeaders']['x-amz-archive-id']
            except botocore.exceptions.ClientError as e:
                print(e)

            # Update dynamodb archive id
            try:
                ann_table = dynamodb.Table(config.get('AWS', 'DynamoDBTable'))
                ann_table.update_item(
                    Key={'job_id': uuid},
                    UpdateExpression="set results_file_archive_id = :a",
                    ExpressionAttributeValues={
                        ':a': archive_id
                    },
                    ReturnValues="UPDATED_NEW"
                )
            except botocore.exceptions.ClientError as e:  # Error - Table does not exist
                print(e)

            # Remove results file from S3
            try:
                s3.delete_object(
                    Bucket=config.get('aws', 'ResultsBucket'),
                    Key=result_file_key)
            except botocore.exceptions.ClientError as e:
                print(f"{e}\nBucket:{config.get('aws', 'ResultsBucket')}\nKey:{result_file_key}")

            # Delete archive message
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


### EOF

if __name__ == '__main__':
    archive_result()
