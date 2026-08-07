module github.com/pennsieve/pennsieve-upload-service-v2/archiver

go 1.24

//replace github.com/pennsieve/pennsieve-go-core => ../../../pennsieve-go-core

require (
	github.com/aws/aws-lambda-go v1.38.0
	github.com/aws/aws-sdk-go-v2 v1.43.4
	github.com/aws/aws-sdk-go-v2/config v1.32.35
	github.com/aws/aws-sdk-go-v2/credentials v1.19.34
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.48
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.22.41
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.59.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.107.0
	github.com/pennsieve/pennsieve-go-core v1.20.0
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.34.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.33.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.38.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.45.4 // indirect
	github.com/aws/smithy-go v1.27.6 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
