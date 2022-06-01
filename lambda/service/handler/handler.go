package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/valyala/fastjson"
)

// DynamoDBDescribeTableAPI defines the interface for the DescribeTable function.
// We use this interface to enable unit testing.
type DynamoDBDescribeTableAPI interface {
	DescribeTable(ctx context.Context,
		params *dynamodb.DescribeTableInput,
		optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// GetTableInfo retrieves information about the table.
func GetTableInfo(c context.Context, api DynamoDBDescribeTableAPI, input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	return api.DescribeTable(c, input)
}

func Handler(request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {

	fmt.Println("Hello event.")

	ApiResponse := events.APIGatewayV2HTTPResponse{}
	// Switch for identifying the HTTP request
	switch request.RequestContext.HTTP.Method {
	case "GET":
		// Obtain the QueryStringParameter
		name := request.QueryStringParameters["name"]
		if name != "" {

			cfg, err := config.LoadDefaultConfig(context.TODO())
			if err != nil {
				panic("unable to load SDK config, " + err.Error())
			}

			// Create an Amazon DynamoDB client.
			client := dynamodb.NewFromConfig(cfg)

			table := aws.String("dev-manifest-files-table-use1")

			// Build the input parameters for the request.
			input := &dynamodb.DescribeTableInput{
				TableName: table,
			}

			resp, err := GetTableInfo(context.TODO(), client, input)
			if err != nil {
				panic("failed to describe table, " + err.Error())
			}

			fmt.Println("Info about " + *table + ":")
			fmt.Println("  #items:     ", resp.Table.ItemCount)
			fmt.Println("  Size (bytes)", resp.Table.TableSizeBytes)
			fmt.Println("  Status:     ", string(resp.Table.TableStatus))

			ApiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        200,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Hey " + name + " welcome! ",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		} else {
			ApiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        500,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Error: Query Parameter name missing",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		}

	case "POST":
		//validates json and returns error if not working
		err := fastjson.Validate(request.Body)

		if err != nil {
			body := "Error: Invalid JSON payload ||| " + fmt.Sprint(err) + " Body Obtained" + "||||" + request.Body
			ApiResponse = events.APIGatewayV2HTTPResponse{Body: body, StatusCode: 500}
		} else {
			ApiResponse = events.APIGatewayV2HTTPResponse{Body: request.Body, StatusCode: 200}
		}
	}
	// Response
	return ApiResponse, nil
}
