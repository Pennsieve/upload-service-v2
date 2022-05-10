# upload-service-v2
Version 2 of the Pennsieve upload service


## Data flow for upload service

1. Client requests upload session ID from API.
2. Client submits mainfest to API.
3. Client uploads files to upload bucket 
4. Bucket triggers event on SQS queue
5. Lambda retrieves one or more events from SQS queue
   1. Lambda verifies uploaded file against manifest
   2. Lambda adds packages to dataset
   3. Lambda moves file to storage bucket