module github.com/pennsieve/pennsieve-upload-service-v2/archiver

go 1.22

toolchain go1.23.4

//replace github.com/pennsieve/pennsieve-go-core => ../../../pennsieve-go-core

require (
	github.com/aws/aws-lambda-go v1.38.0
	github.com/aws/aws-sdk-go-v2 v1.17.8
	github.com/aws/aws-sdk-go-v2/config v1.18.18
	github.com/aws/aws-sdk-go-v2/credentials v1.13.17
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.14
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.58
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.19.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.30.6
	github.com/pennsieve/pennsieve-go-core v1.13.0
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.32 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.14.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.14.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.18.6 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
