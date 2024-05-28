## Description
This is a scalable bioinformatic genomics annotation microservice that featured aws automated scaling and secure data handling.
•	Utilized AWS service such as EC2 in hosting frontend and backend services, leveraging autoscaling groups with ELB to ensure high availability and scalability.
•	Implemented robust user authentication and premium subscription features through Flask and Globus Auth.
•	Leveraged DynamoDB, PostgreSQL, Glacier and S3 for data storage, Lambda for real-time user email notifications, and SNS/SQS for async inter-service communication.
•	Ensured high availability and performance under load that effectively improved operational efficiency and user satisfaction.

## Website Preview
![gas-1.png](aws%2Fgas-1.png)
![gas-2.png](aws%2Fgas-2.png)
![gas-3.png](aws%2Fgas-3.png)
![gas-4.png](aws%2Fgas-4.png)

## Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Archive Process
* In `run.py`, checks if the current user profile and if current user is free-tier, then send message to `ishaz_glacier_archive` SQS queue to start the archive process.
* In `archive,py`, there is a while loop continuously read a maximum of 5 sqs messages from the archive SQS queue in a row, with long polling of 20 seconds.
  * If it receives a message, it then checks again if the user is a free user, accounting for the case where a user upgrades from free to premium after they submit an annotation job, before their results are being archived.
  * If the user is a free user, then it gets userid, jobid, s3_result_key from the message body. In several try-catch blocks, it first tries to get the result file from s3, and archive the file from s3 to glacier. After updating dynamodb with its new archive id through querying with its jobid, it deletes the job file on s3, and finally delete the sqs message to indicate job done.
## Restore Process
* In `subscribe` function in `views.py`, if the user subscribes to the premium tier, it sends a message to `ishaz_glacier_restore` SQS queue to start the restore process.
* In `restore.py`, there is also a while loop continuously read a maximum of 5 sqs messages from the archive SQS queue in a row, with long polling of 20 seconds.
  * If it receives a message, it then gets the userid from the message, and query all annotation jobs associated with this userid. In a for loop, the function checks if this job has an archive id but has not been restored. 
  * If it's in this case, the function will initiate a glacier job of type 'archive_retrieval'. The retrieval job will first attempt to use the expedited version, and degrade to standard version otherwise.
  * If all actions performing successfully, the message will be deleted from the sqs queue to indicate job done.
