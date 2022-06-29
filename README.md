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

## Create Lambda Build
Prior to terraforming the Lambda (which zips and uploads the lambda to AWS), the Lambda function needs
to be build for the Lambda environment. You can do this with the following command:

```env GOOS=linux GOARCH=amd64 go build -o ../bin/moveTrigger/pennsieve_move_trigger```

```env GOOS=linux GOARCH=amd64 go build -o ../bin/handler/pennsieve_upload_handler```

```env GOOS=linux GOARCH=amd64 go build -o ../bin/service/pennsieve_upload_service```