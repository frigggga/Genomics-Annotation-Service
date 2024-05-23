# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
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
## Additional Notes
* All required tasks are implemented.
* No attempt to do the optional tasks.
