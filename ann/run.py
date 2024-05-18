import sys
import time
import boto3
import os
import botocore
from configparser import ConfigParser
import driver

"""A rudimentary timer for coarse-grained profiling
"""

CONFIG_FILE = '/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini'
config = ConfigParser()
config.read_file(open(CONFIG_FILE))


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


# source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html
if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        s3_client = boto3.client('s3')
        bucket_name = config.get('AWS', 'ResultsBucket')
        files = os.listdir(sys.argv[2])
        user_id = sys.argv[3]
        job_id = sys.argv[4]
        user_email = sys.argv[5]

        for filename in files:
            local_path = os.path.join(sys.argv[2], filename)
            if filename.endswith('.annot.vcf') or filename.endswith('.vcf.count.log'):
                s3_path = f"{config.get('AWS', 'Owner')}/{user_id}/{job_id}~{filename}"
                try:
                    # Upload the file to S3
                    s3_client.upload_file(local_path, bucket_name, s3_path)
                    print(f'Successfully uploaded {filename} to s3://{bucket_name}/{s3_path}')
                except Exception as e:
                    print(f'Error processing file {filename}: {str(e)}')

            os.remove(local_path)

        try:
            os.rmdir(sys.argv[2])
            print(f"The directory {sys.argv[2]} has been deleted successfully")
        except OSError as e:
            print(f"Error: {e.strerror}")

        # update the dynamoDB table status to COMPLETED
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
        try:
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            ann_table = dynamodb.Table(config.get('AWS', 'DynamoDBTable'))
            table_fields = ann_table.get_item(Key={'job_id': job_id})
            response = ann_table.get_item(Key={'job_id': job_id})
            input_file_name = response['Item']['s3_key_input_file']
            result_file_name = input_file_name[:-4] + '.annot.vcf'
            log_file_name = input_file_name[:-4] + '.vcf.count.log'

            ann_table.update_item(Key={'job_id': job_id},
                                  UpdateExpression="set job_status = :s, s3_results_bucket = :b, s3_key_result_file = :f, s3_key_log_file = :l, complete_time = :c",
                                  ExpressionAttributeValues={
                                      ':s': 'COMPLETED',
                                      ':b': bucket_name,
                                      ':f': result_file_name,
                                      ':l': log_file_name,
                                      ':c': int(time.time())
                                  },
                                  ReturnValues="UPDATED_NEW"
                                  )
        except botocore.exceptions.ClientError:
            print('error updating table.')

        # Send user email notifications
        sns = boto3.client('sns')
        message = str({
            'filename': sys.argv[1],
            'job_id': job_id,
            'user_email': user_email,
            'job_status': 'COMPLETED'

        })

        try:  # Publish SNS message
            response = sns.publish(
                TopicArn=config.get('AWS', 'AWS_SNS_JOB_COMPLETE_TOPIC'),
                Message=message
            )
        except botocore.exceptions.ClientError as e:  # Topic not found
            print({
                'code': 'HTTP_500_INTERNAL_SERVER_ERROR',
                'status': 'error',
                'message': 'Error publishing file annotation results to sqs queue'
            })

    else:
        print("A valid .vcf file must be provided as input to this program.")
