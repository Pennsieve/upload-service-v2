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

## Testing

The tests are automatically run by Jenkins once you push to a feature branch. Successful tests are required to merge a feature branch into the main branch.

## Deployment

__Build and Development Deployment__

Artifacts are built in Jenkins and published to S3. The dev build triggers a deployment of the Lambda function and creates a "Lambda version" that is used by the model-service.

__Deployment of an Artifact__

1. Deployements to *development* are automatically done by Jenkins once you merge a feature branch into main.

2. Deployments to *production* are done via Jenkins.

   1. Determine the artifact version you want to deploy (you can find the latest version number in the development deployment job).
   2. Run the production deployment task with the new IMAGE_TAG