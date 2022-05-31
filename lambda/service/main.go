package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/pennsieve-upload-service-v2/service/handler"
)

func main() {
	lambda.Start(handler.Handler)
}
