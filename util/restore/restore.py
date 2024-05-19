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
config.read('restore_config.ini')

# Add utility code here

def restore():
    try:
        sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
        queue_name = config.get('aws', 'SQSRestoreQueueName')
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

        # Get list of annotations to display
        try:
            ann_table = dynamodb.Table(config['aws']['DynamoDBTable'])
            response = ann_table.query(
                IndexName='user_id_index',
                KeyConditionExpression=Key('user_id').eq(user_id)
            )

        except botocore.exceptions.ClientError as e:
            print(e)

        jobs = response['Items']
        for job in jobs:
            if 'archive_id' in job.keys() and job['is_restored'] is False:

                description = str({
                    'job_id': job['job_id'],
                    's3_key_result_file': job['s3_key_result_file']
                })

                job_parameters = {
                    'Type': 'archive-retrieval',
                    'ArchiveId': job['archive_id'],
                    'SNSTopic': config['aws']['SNSThawTopic'],
                    'Tier': 'Expedited',
                    'Description': description
                }

                # Create glacier restore jobs
                try:
                    job_response = glacier.initiate_job(vaultName=config.get('VaultName'), jobParameters=job_parameters)
                    restore_job_id = job_response['jobId']

                except:  # Graceful degradation
                    try:
                        response = glacier.initiate_job(
                            vaultName=config['aws']['VAULT_NAME'],
                            jobParameters={
                                'Type': "archive-retrieval",
                                'ArchiveId': job['archive_id'],
                                'SNSTopic': config['aws']['SNSThawTopic'],
                                'Tier': 'Standard',
                                'Description': description
                            }
                        )
                    except botocore.exceptions.ClientError as e:
                        print(e)
                        continue

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

### EOF


if __name__ == '__main__':
    restore()
