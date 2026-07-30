module github.com/pennsieve/pennsieve-upload-service-v2/service

go 1.24.0

//replace (
//	github.com/pennsieve/pennsieve-go-core => ../../../pennsieve-go-core
//)

replace github.com/pennsieve/pennsieve-upload-service-v2/pkg/bucketregion => ../../pkg/bucketregion

require (
	github.com/aws/aws-lambda-go v1.46.0
	github.com/aws/aws-sdk-go-v2 v1.42.0
	github.com/aws/aws-sdk-go-v2/config v1.32.25
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.48
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.59.0
	github.com/google/uuid v1.6.0
	github.com/pennsieve/pennsieve-go-core v1.19.1
	github.com/pennsieve/pennsieve-upload-service-v2/pkg/bucketregion v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.3
	github.com/valyala/fastjson v1.6.4
)

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.19.24
	github.com/aws/aws-sdk-go-v2/service/lambda v1.53.2
	github.com/aws/aws-sdk-go-v2/service/s3 v1.104.0
	github.com/aws/aws-sdk-go-v2/service/sqs v1.44.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.43.3
	github.com/aws/smithy-go v1.27.1
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.13 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.29 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.30 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.34.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.2.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.31.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.36.6 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
