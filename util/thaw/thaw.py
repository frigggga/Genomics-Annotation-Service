# thaw.py
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
config.read('thaw_config.ini')

# Add utility code here
def thaw():

    try:
        sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
        queue_name = config.get('aws', 'SQSThawQueueName')
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
                QueueUrl=queue_url, AttributeNames=['All'], MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )
        except KeyError:
            # since there's no messages in the queue, keep listening...
            continue

        # Check #num messages received
        try:
            messages = queue['Messages']
            if len(messages) == 0:
                continue
        except KeyError:
            continue

        for m in messages:
            receipt_handle = m["ReceiptHandle"]
            message_json = ast.literal_eval(m["Body"])
            message = json.loads(message_json["Message"])
            restore_job_id = message['JobId']
            description = ast.literal_eval(message['JobDescription'])
            job_id = description['job_id']
            s3_result_key = description['s3_key_result_file']
            print('---message received------')

            # Get job archive data
            try:
                job_status = glacier.describe_job(vaultName=config['aws']['VaultName'], jobId=restore_job_id)

                if job_status['Completed']:
                    job_output = glacier.get_job_output(vaultName=config['aws']['VaultName'], jobId=restore_job_id)
                    archive_data = job_output['body'].read()
                    print('---get archived data from galcier------')

            except botocore.exceptions.ClientError as e:
                print(e)
                continue

            # Upload file to s3
            try:
                s3.put_object(
                    Body=archive_data,
                    Bucket=config['aws']['ResultsBucket'],
                    Key=s3_result_key
                )
                print('---uploaded file to s3------')
            except botocore.exceptions.ClientError as e:
                print(e)
                continue

            # Update Dynamodb is_restored status to true
            try:
                ann_table = dynamodb.Table(config['aws']['DynamoDBTable'])
                ann_table.update_item(Key={'job_id': job_id},
                                      UpdateExpression="set is_restored = :r",
                                      ExpressionAttributeValues={
                                          ':r': True
                                      },
                                      ReturnValues="UPDATED_NEW"
                                      )
                print('---dynamodb is_restored updated------')

            except botocore.exceptions.ClientError as e:
                print(e)
                continue

            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print('message deleted')


if __name__ == '__main__':
    thaw()

### EOF