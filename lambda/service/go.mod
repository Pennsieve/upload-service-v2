module github.com/pennsieve/pennsieve-upload-service-v2/service

go 1.18

//replace github.com/pennsieve/pennsieve-go-api => ../../../pennsieve-go-api

require (
	github.com/aws/aws-lambda-go v1.32.0
	github.com/aws/aws-sdk-go-v2 v1.16.6
	github.com/aws/aws-sdk-go-v2/config v1.15.9
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.9.4
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.15.7
	github.com/google/uuid v1.3.0
	github.com/pennsieve/pennsieve-go-api v1.1.1
	github.com/valyala/fastjson v1.6.3
)

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.12.4 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.1.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.17.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.6 // indirect
	github.com/aws/smithy-go v1.12.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/lib/pq v1.10.6 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
