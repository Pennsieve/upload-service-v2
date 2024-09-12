package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/handler"
)

func init() {
	handler.InitializeClients()
}
func main() {
	lambda.Start(handler.Handler)
}
