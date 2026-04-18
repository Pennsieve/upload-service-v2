package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/pennsieve-upload-service-v2/archive-sweeper/handler"
)

func main() {
	handler.InitializeClients()
	lambda.Start(handler.Handle)
}
