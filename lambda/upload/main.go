package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/handler"
)

func main() {
	fmt.Println("test lambda")
	lambda.Start(handler.Handler)
}
