# Lambda(s)

This folder contains one folder per Lambda function. 


## Building Lambdas for Terraform 
Each Lambda should be build using:

```env GOOS=linux GOARCH=amd64 go build -o ../bin/pennsieve_upload_service_v2_<lambda_name>```