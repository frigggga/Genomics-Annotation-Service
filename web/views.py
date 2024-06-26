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
import pytz
from datetime import datetime

import boto3
import os
import botocore
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
                   request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
s3 = boto3.resource('s3',
                    region_name=app.config['AWS_REGION_NAME'],
                    config=Config(signature_version='s3v4'))

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

    s3 = boto3.client('s3',
                      region_name=app.config['AWS_REGION_NAME'],
                      config=Config(signature_version='s3v4'))

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

    # Extract the job ID from the S3 key
    owner, _, uu_id_and_file_name = s3_key.split('/')
    uu_id, file_name = uu_id_and_file_name.split('~')
    user_id = session['primary_identity']  # Globus Identity ID is a UUID

    if not file_name.endswith('.vcf'):  # File still gets uploaded though
        abort(405)

    if uu_id == None or file_name == None or file_name == "":
        abort(405)

    # Persist job to database
    ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    data = {
        "job_id": str(uu_id),
        "user_id": str(user_id),
        'input_file_name': str(file_name),
        's3_inputs_bucket': bucket_name,
        's3_key_input_file': s3_key,
        'submit_time': int(time.time()),
        'job_status': 'PENDING'
    }

    try:
        ann_table.put_item(
            Item=data,
            ConditionExpression='attribute_not_exists(job_id)'
        )
    except botocore.exceptions.ClientError as e:  # Job was already uploaded to cloud
        code = e.response['Error']['Code']
        if code == 'ConditionalCheckFailedException':
            abort(405)
        else:
            abort(500)

    # Send message to request queue
    try:
        user_email = session['email']
        data['user_email'] = str(user_email)
        sns.publish(
            TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
            Message=str(data)
        )
    except botocore.exceptions.ClientError as e:  # Topic not found
        code = e.response['Error']['Code']
        if code == 'NotFound':
            abort(404)
        else:
            abort(500)

    return render_template('annotate_confirm.html', job_id=uu_id)


"""List all annotations for the user
"""


#  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#querying-and-scanning
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # Get list of annotations to display
    try:
        ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        user_id = session['primary_identity']
        response = ann_table.query(
            IndexName='user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id),
            ProjectionExpression="job_id, submit_time, input_file_name, job_status"
        )

    except botocore.exceptions.ClientError:
        abort(500)

    jobs = response['Items']
    for job in jobs:
        job['submit_time'] = get_chicago_time(job['submit_time'])

    return render_template('annotations.html', annotations=jobs)


"""Display details of a specific annotation job
"""


@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    try:
        ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        response = ann_table.query(
            KeyConditionExpression=Key('job_id').eq(id)
        )
        annotation = response['Items'][0]

    except botocore.exceptions.ClientError:
        abort(500)

    if annotation['user_id'] != session['primary_identity']:
        abort(403)

    free_access_expired = False
    annotation['submit_time'] = get_chicago_time(annotation['submit_time'])

    if annotation['job_status'] == 'COMPLETED':
        complete_time = float(annotation['complete_time'])
        annotation['complete_time'] = complete_time

        if (session['role'] == 'free_user' and time.time() - annotation['complete_time'] >= app.config['FREE_USER_DATA_RETENTION']):
            free_access_expired = True

        if 'is_restored' in annotation.keys() and annotation['is_restored'] is False and session['role'] != 'free_user':
            annotation['restore_message'] = \
                "Your result files are currently in the restore process, please be patient while waiting."

        annotation['complete_time'] = get_chicago_time(annotation['complete_time'])
        annotation['result_file_url'] = create_presigned_download_url(annotation['s3_key_result_file'])

    return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.get
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    bucket_name = app.config['AWS_S3_RESULTS_BUCKET']

    try:
        ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        response = ann_table.query(
            KeyConditionExpression=Key('job_id').eq(id)
        )
        annotation = response['Items'][0]

    except botocore.exceptions.ClientError:
        abort(500)

    if annotation['user_id'] != session['primary_identity']:
        abort(403)

    s3_key = annotation['s3_key_log_file']

    try:
        object = s3.Object(bucket_name, s3_key)
        content = object.get()['Body'].read().decode()

    except botocore.exceptions.ClientError as e:
        app.logger.info(e)
        abort(500)

    return render_template('view_log.html', job_id=id, log_file_contents=content)


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

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Make sure you handle files not yet archived!

        message = str({
            'user_id': session['primary_identity']
        })

        # Publish SNS message to restore queue
        try:
            sns.publish(
                TopicArn=app.config['AWS_SNS_RESTORE_TOPIC'],
                Message=message
            )
        except botocore.exceptions.ClientError as e:  # Topic not found
            print({
                'code': 'HTTP_500_INTERNAL_SERVER_ERROR',
                'status': 'error',
                'message': 'Error publishing restoring requests to sqs queue'
            })

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


# Helper Functions

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
def create_presigned_download_url(result_object_key):
    s3 = boto3.client('s3',
                      region_name=app.config['AWS_REGION_NAME'],
                      config=Config(signature_version='s3v4'))

    result_bucket = app.config['AWS_S3_RESULTS_BUCKET']

    # Generate the presigned POST call
    try:
        response = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': result_bucket,
                'Key': result_object_key},
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    return response


def get_chicago_time(timestamp):
    utc_time = datetime.utcfromtimestamp(float(timestamp)).replace(tzinfo=pytz.utc)
    chicago_tz = pytz.timezone('America/Chicago')
    chicago_time = utc_time.astimezone(chicago_tz)
    return chicago_time.strftime('%a %b %d %H:%M:%S %Y')

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
