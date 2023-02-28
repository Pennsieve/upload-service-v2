package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dbTable"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/gateway"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	manifestPkg "github.com/pennsieve/pennsieve-go-core/pkg/upload"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"os"
	"strconv"
	"time"
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

// getManifestRoute returns a list of manifests for a given dataset
func getManifestRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	// Create an Amazon DynamoDB client.
	table := os.Getenv("MANIFEST_TABLE")
	var activeManifest *dbTable.ManifestTable
	manifests, err := activeManifest.GetManifestsForDataset(client, table, claims.DatasetClaim.NodeId)
	if err != nil {
		message := "Error: Unable to get manifests for dataset: " + claims.DatasetClaim.NodeId + " ||| " + fmt.Sprint(err)
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// Build the input parameters for the request.

	var manifestDTOs []manifest.ManifestDTO
	for _, m := range manifests {
		manifestDTOs = append(manifestDTOs, manifest.ManifestDTO{
			Id:            m.ManifestId,
			DatasetNodeId: m.DatasetNodeId,
			DatasetId:     m.DatasetId,
			Status:        m.Status,
			User:          m.UserId,
			DateCreated:   m.DateCreated,
		})
	}

	responseBody := manifest.GetResponse{
		Manifests: manifestDTOs,
	}

	headers := map[string]string{
		"Access-Control-Allow-Headers": "Content-Type, Authorization",
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Methods": "OPTIONS,POST,GET",
	}

	jsonBody, _ := json.Marshal(responseBody)
	apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200, Headers: headers}

	return &apiResponse, nil
}

// postManifestRoute synchronizes manifests with a provided ID
func postManifestRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	fmt.Println("Handling POST /manifest request")

	apiResponse := events.APIGatewayV2HTTPResponse{}

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

		manifestId := uuid.New().String()

		log.WithFields(
			log.Fields{
				"dataset_id":  claims.DatasetClaim.NodeId,
				"manifest_id": manifestId,
				"user_id":     claims.UserClaim.Id,
			},
		).Info("Creating new manifest.")

		// Create new manifest
		activeManifest = &dbTable.ManifestTable{
			ManifestId:     manifestId,
			DatasetId:      claims.DatasetClaim.IntId,
			DatasetNodeId:  claims.DatasetClaim.NodeId,
			OrganizationId: claims.OrgClaim.IntId,
			UserId:         claims.UserClaim.Id,
			Status:         manifest.Initiated.String(),
			DateCreated:    time.Now().Unix(),
		}

		activeManifest.CreateManifest(s.Client, s.TableName, *activeManifest)

	} else {
		// Check that manifest exists.
		log.Debug("Has existing manifest")

		//activeManifest, err = store.GetFromManifest(context.Background(), store.tableName, res.ID)
		if err != nil {
			message := "Error: Invalid ManifestID |||| Manifest ID: " + res.ID
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
			return &apiResponse, nil
		}

	}

	// MERGE PACKAGES FOR SPECIFIC FILETYPES
	// TODO: Improve this functionality to handle situation where part of the merged package is previously added to the manifest.
	// Currently, the merge only happens within the files included in the call.
	s.PackageTypeResolver(res.Files)

	// ADDING FILES TO MANIFEST
	addFilesResponse := s.AddFiles(activeManifest.ManifestId, res.Files, nil)

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

	return &apiResponse, nil
}

// getManifestFilesRoute returns a paginated list of files for a manifest with a provided ID
func getManifestFilesRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}
	queryParams := request.QueryStringParameters

	var manifestId string
	var found bool
	if manifestId, found = queryParams["manifest_id"]; !found {
		message := "Error: ManifestID not specified"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}

	// Create an Amazon DynamoDB client.
	client := dynamodb.NewFromConfig(cfg)

	table := os.Getenv("MANIFEST_FILE_TABLE")

	var limit int32
	if v, found := queryParams["limit"]; found {
		r, err := strconv.ParseInt(v, 10, 32)
		limit = int32(r)
		if err != nil {
			return nil, err
		}
	} else {
		limit = int32(20)
	}

	status := sql.NullString{}
	if v, found := queryParams["status"]; found {
		status = sql.NullString{
			String: v,
			Valid:  true,
		}
	}

	var startKey map[string]types.AttributeValue
	if v, found := queryParams["continuation_token"]; found {
		if status.Valid {
			startKey = map[string]types.AttributeValue{
				"ManifestId": &types.AttributeValueMemberS{Value: manifestId},
				"UploadId":   &types.AttributeValueMemberS{Value: v},
				"Status":     &types.AttributeValueMemberS{Value: status.String},
			}
		} else {
			startKey = map[string]types.AttributeValue{
				"ManifestId": &types.AttributeValueMemberS{Value: manifestId},
				"UploadId":   &types.AttributeValueMemberS{Value: v},
			}
		}
	}

	var mf *dbTable.ManifestFileTable
	manifestFiles, lastKey, err := mf.GetFilesPaginated(client, table, manifestId, status, limit, startKey)
	if err != nil {
		message := "Error: Unable to get files for manifests: " + manifestId + " ||| " + fmt.Sprint(err)
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// Build the input parameters for the request.
	var manifestFilesDTO []manifestFile.DTO
	for _, m := range manifestFiles {

		pt := packageType.FileTypeToInfoDict[fileType.Dict[m.FileType]]
		manifestFilesDTO = append(manifestFilesDTO, manifestFile.DTO{
			FileName: m.FileName,
			FilePath: m.FilePath,
			FileType: m.FileType,
			UploadId: m.UploadId,
			Status:   m.Status,
			Icon:     pt.Icon.String(),
		})
	}

	var lastUploadId string
	_ = attributevalue.Unmarshal(lastKey["UploadId"], &lastUploadId)

	responseBody := manifestFile.GetManifestFilesResponse{
		ManifestId:        manifestId,
		Files:             manifestFilesDTO,
		ContinuationToken: lastUploadId,
	}

	jsonBody, _ := json.Marshal(responseBody)
	apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200}

	return &apiResponse, nil
}

// getManifestFilesStatusRoute Returns a list of upload-ids associated with a specified manifest and status
//
// This method returns a list of file ids for the given manifest and status.
// If the "verify" flag is set in the request, then the requested status is always set to "Finalized" and the status
// for the returned files is updated to "Verfied". This enables the workflow for the agent to verify completed uploads
// and indicate that they uploads were verified by the client.
func getManifestFilesStatusRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

	/*
		Parse inputs
	*/

	apiResponse := events.APIGatewayV2HTTPResponse{}
	queryParams := request.QueryStringParameters

	// Get Manifest ID
	var manifestId string
	var found bool
	if manifestId, found = queryParams["manifest_id"]; !found {
		message := "Error: ManifestID not specified"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	// Get Status
	// Check "Verify" flag
	var statusStr string
	var verifyFlag string
	updateStatus := false
	if verifyFlag, found = queryParams["verify"]; found {
		updateStatus = verifyFlag == "true"
	}

	// Check "Status" query input
	if statusStr, found = queryParams["status"]; !found {
		message := "Error: status not specified"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	if updateStatus {
		// Assert that the user requested the "Finalized" status
		if statusStr != manifestFile.Finalized.String() {
			message := "Error: Status parameter needs to be set to 'Finalized' when verifying upload."
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
			return &apiResponse, nil
		}

		statusStr = "Finalized"
	}

	status := sql.NullString{
		String: statusStr,
		Valid:  true,
	}

	// Get Continuation Key
	var startKey map[string]types.AttributeValue
	if v, found := queryParams["continuation_token"]; found {
		startKey = map[string]types.AttributeValue{
			"ManifestId": &types.AttributeValueMemberS{Value: manifestId},
			"UploadId":   &types.AttributeValueMemberS{Value: v},
			"Status":     &types.AttributeValueMemberS{Value: status.String},
		}
	}

	/*
		Query table
	*/
	var mf *dbTable.ManifestFileTable
	files, lastKey, err := mf.GetFilesPaginated(client, manifestFileTableName, manifestId, status, 500, startKey)
	if err != nil {
		message := "Error: Unable to get manifests for dataset: " + claims.DatasetClaim.NodeId + " ||| " + fmt.Sprint(err)
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// Build the input parameters for the request.
	var UploadIds []string
	for _, f := range files {
		UploadIds = append(UploadIds, f.UploadId)
	}

	// Update status for returned items to "Verified"
	if updateStatus {
		for _, f := range UploadIds {
			err = mf.UpdateFileTableStatus(client, manifestFileTableName, manifestId, f, manifestFile.Verified, "")
			if err != nil {
				fmt.Println("Error updating table: ", err)
			}
		}
	}

	// Only return UploadID of lastKey as the rest can be inferred from call.
	var lastUploadId string
	_ = attributevalue.Unmarshal(lastKey["UploadId"], &lastUploadId)

	responseBody := manifest.GetStatusEndpointResponse{
		ManifestId:        manifestId,
		Status:            status.String,
		Files:             UploadIds,
		ContinuationToken: lastUploadId,
		Verified:          updateStatus,
	}

	jsonBody, _ := json.Marshal(responseBody)
	apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200}

	return &apiResponse, nil
}

func handleManifestIdRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

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

func handleManifestIdUpdatesRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

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

func handleManifestIdRemoveRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	switch request.RequestContext.HTTP.Method {
	case "DELETE":
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
	}

	return &apiResponse, nil

}
