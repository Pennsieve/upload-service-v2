package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/gateway"
	"github.com/pennsieve/pennsieve-go-api/models/manifest"
	manifestPkg "github.com/pennsieve/pennsieve-go-api/pkg/manifest"
	"github.com/valyala/fastjson"
	"log"
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

func handleManifestRoute(request events.APIGatewayV2HTTPRequest, claims *Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	switch request.RequestContext.HTTP.Method {
	// Get a list of active manifest for user.
	case "GET":

		// Obtain the QueryStringParameter
		name := request.QueryStringParameters["name"]

		if name != "" {

			cfg, err := config.LoadDefaultConfig(context.Background())
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
			fmt.Println("  ClientStatus:     ", string(resp.Table.TableStatus))

			apiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        200,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Hey " + name + " welcome! ",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        500,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Error: Query Parameter name missing",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		}
		fmt.Println("Handling GET /manifest request")

	// Sync files in manifest
	case "POST":
		fmt.Println("Handling POST /manifest request")

		// PARSING INPUTS
		//validates json and returns error if not working
		err := fastjson.Validate(request.Body)
		if err != nil {
			message := "Error: Invalid JSON payload ||| " + fmt.Sprint(err) + " Body Obtained" + "||||" + request.Body
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
			return &apiResponse, nil
		}

		// Unmarshal JSON into Manifest DTOs
		bytes := []byte(request.Body)
		var res manifest.DTO
		json.Unmarshal(bytes, &res)

		fmt.Println("SessionID: ", res.ID, " NrFiles: ", len(res.Files))

		s := manifestPkg.ManifestSession{
			FileTableName: manifestFileTableName,
			TableName:     manifestTableName,
			Client:        client,
		}

		// ADDING MANIFEST IF NEEDED
		var activeManifest *dbTable.ManifestTable
		if res.ID == "" {
			log.Printf("Creating new manifest")
			// Create new manifest
			activeManifest = &dbTable.ManifestTable{
				ManifestId:     uuid.New().String(),
				DatasetId:      claims.datasetClaim.IntId,
				DatasetNodeId:  claims.datasetClaim.NodeId,
				OrganizationId: claims.organizationId,
				UserId:         claims.userId,
				Status:         manifest.Initiated.String(),
			}

			s.CreateManifest(*activeManifest)

		} else {
			// Check that manifest exists.
			log.Printf("Has existing manifest")

			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				return nil, fmt.Errorf("LoadDefaultConfig: %v\n", err)
			}

			// Create an Amazon DynamoDB client.
			client := dynamodb.NewFromConfig(cfg)

			activeManifest, err = dbTable.GetFromManifest(client, manifestTableName, res.ID)
			if err != nil {
				message := "Error: Invalid ManifestID |||| Manifest ID: " + res.ID
				apiResponse = events.APIGatewayV2HTTPResponse{
					Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
				return &apiResponse, nil
			}

		}

		// ADDING FILES TO MANIFEST
		addFilesResponse, err := s.AddFiles(activeManifest.ManifestId, res.Files, nil)

		// CREATING API RESPONSE
		responseBody := manifest.PostResponse{
			ManifestNodeId: activeManifest.ManifestId,
			UpdatedFiles:   addFilesResponse.FileStatus,
			NrFilesUpdated: addFilesResponse.NrFilesUpdated,
			NrFilesRemoved: addFilesResponse.NrFilesRemoved,
			FailedFiles:    addFilesResponse.FailedFiles,
		}

		jsonBody, _ := json.Marshal(responseBody)
		apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200}

	}

	return &apiResponse, nil

}

func handleManifestIdRoute(request events.APIGatewayV2HTTPRequest, claims *Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	switch request.RequestContext.HTTP.Method {
	case "GET":
		// Obtain the QueryStringParameter
		name := request.QueryStringParameters["name"]

		if name != "" {

			cfg, err := config.LoadDefaultConfig(context.Background())
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
			fmt.Println("  ClientStatus:     ", string(resp.Table.TableStatus))

			apiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        200,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Hey " + name + " welcome! ",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{
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
			apiResponse = events.APIGatewayV2HTTPResponse{Body: body, StatusCode: 500}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{Body: request.Body, StatusCode: 200}
		}
	}

	return &apiResponse, nil

}

func handleManifestIdUpdatesRoute(request events.APIGatewayV2HTTPRequest, claims *Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	switch request.RequestContext.HTTP.Method {
	case "GET":
		// Obtain the QueryStringParameter
		name := request.QueryStringParameters["name"]

		if name != "" {

			cfg, err := config.LoadDefaultConfig(context.Background())
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
			fmt.Println("  ClientStatus:     ", string(resp.Table.TableStatus))

			apiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        200,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Hey " + name + " welcome! ",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{
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
			apiResponse = events.APIGatewayV2HTTPResponse{Body: body, StatusCode: 500}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{Body: request.Body, StatusCode: 200}
		}
	}

	return &apiResponse, nil

}

func handleManifestIdRemoveRoute(request events.APIGatewayV2HTTPRequest, claims *Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	switch request.RequestContext.HTTP.Method {
	case "GET":
		// Obtain the QueryStringParameter
		name := request.QueryStringParameters["name"]

		if name != "" {

			cfg, err := config.LoadDefaultConfig(context.Background())
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
			fmt.Println("  ClientStatus:     ", string(resp.Table.TableStatus))

			apiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        200,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Hey " + name + " welcome! ",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{
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
			apiResponse = events.APIGatewayV2HTTPResponse{Body: body, StatusCode: 500}
		} else {
			apiResponse = events.APIGatewayV2HTTPResponse{Body: request.Body, StatusCode: 200}
		}
	}

	return &apiResponse, nil

}
