package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	types2 "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/gateway"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/upload"
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

// getManifestRoute returns a list of manifests for a given dataset
func getManifestRoute(_ events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

	apiResponse := events.APIGatewayV2HTTPResponse{}

	// Create an Amazon DynamoDB client.
	table := os.Getenv("MANIFEST_TABLE")
	fileTable := os.Getenv("MANIFEST_FILE_TABLE")
	ctx := context.Background()

	manifests, err := store.dy.GetManifestsForDataset(ctx, table, claims.DatasetClaim.NodeId)
	if err != nil {
		message := "Error: Unable to get manifests for dataset: " + claims.DatasetClaim.NodeId + " ||| " + fmt.Sprint(err)
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// Build the input parameters for the request.
	var manifestDTOs []manifest.ManifestDTO
	for _, m := range manifests {

		// If the manifest is not marked as completed, check for completeness
		var s manifest.Status
		mStatus := s.ManifestStatusMap(m.Status)
		if m.Status != manifest.Completed.String() {
			mStatus, err = store.dy.CheckUpdateManifestStatus(ctx, fileTable, table, m.ManifestId, m.Status)
			if err != nil {
				log.Error(err)
			}
		}

		manifestDTOs = append(manifestDTOs, manifest.ManifestDTO{
			Id:            m.ManifestId,
			DatasetNodeId: m.DatasetNodeId,
			DatasetId:     m.DatasetId,
			Status:        mStatus.String(),
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
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	// Unmarshal JSON into Manifest DTOs
	bytes := []byte(request.Body)
	var res manifest.DTO
	err = json.Unmarshal(bytes, &res)
	if err != nil {
		message := "Error: Invalid JSON payload ||| " + fmt.Sprint(err) + " Body Obtained" + "||||" + request.Body
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	//fmt.Println("SessionID: ", res.ID, " NrFiles: ", len(res.Files))

	// ADDING MANIFEST IF NEEDED
	var activeManifest *dydb.ManifestTable
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
		activeManifest = &dydb.ManifestTable{
			ManifestId:     manifestId,
			DatasetId:      claims.DatasetClaim.IntId,
			DatasetNodeId:  claims.DatasetClaim.NodeId,
			OrganizationId: claims.OrgClaim.IntId,
			UserId:         claims.UserClaim.Id,
			Status:         manifest.Initiated.String(),
			DateCreated:    time.Now().Unix(),
		}

		err := store.dy.CreateManifest(context.Background(), store.tableName, *activeManifest)
		if err != nil {
			message := "Error: Could not create manifest |||| Manifest ID: " + res.ID
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
			return &apiResponse, nil
		}

	} else {
		// Check that manifest exists.
		log.Debug("Has existing manifest")

		activeManifest, err = store.dy.GetManifestById(context.Background(), store.tableName, res.ID)
		if err != nil {
			message := "Error: Invalid ManifestID |||| Manifest ID: " + res.ID
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
			return &apiResponse, nil
		}

		// Check that manifest is not archived.
		if activeManifest.Status == manifest.Archived.String() {
			message := "Cannot sync with an 'archived' manifest. Archived manifests can be downloaded as a CSV file."
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
			return &apiResponse, nil
		}
	}

	// MERGE PACKAGES FOR SPECIFIC FILETYPES
	// TODO: Improve this functionality to handle situation where part of the merged package is previously added to the manifest.
	// Currently, the merge only happens within the files included in the call.
	upload.PackageTypeResolver(res.Files)

	// ADDING FILES TO MANIFEST
	addFilesResponse, err := store.dy.SyncFiles(activeManifest.ManifestId, res.Files, nil, store.tableName, store.fileTableName)
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifestId": activeManifest.ManifestId,
				"datasetId":  claims.DatasetClaim.NodeId,
			},
		).Error("Error syncing files:", err)
		message := "Error: cannot sync files with manifest: " + activeManifest.ManifestId
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// CREATING API RESPONSE
	responseBody := manifest.PostResponse{
		ManifestNodeId: activeManifest.ManifestId,
		UpdatedFiles:   addFilesResponse.FileStatus,
		NrFilesUpdated: addFilesResponse.NrFilesUpdated,
		NrFilesRemoved: addFilesResponse.NrFilesRemoved,
		FailedFiles:    addFilesResponse.FailedFiles,
	}

	// If the manifest is not marked as completed, check for completeness if sync is not adding files
	if activeManifest.Status != manifest.Completed.String() && addFilesResponse.NrFilesUpdated == 0 {
		_, err = store.dy.CheckUpdateManifestStatus(context.Background(), store.fileTableName, store.tableName, activeManifest.ManifestId, activeManifest.Status)
		if err != nil {
			log.Error(fmt.Sprintf("Could not check/update Manifest Status: %v", err))
		}
	}

	jsonBody, _ := json.Marshal(responseBody)
	apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200}

	return &apiResponse, nil
}

// getManifestFilesRoute returns a paginated list of files for a manifest with a provided ID
func getManifestFilesRoute(request events.APIGatewayV2HTTPRequest, _ *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {

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

	// Check that Manifest exists
	activeManifest, err := store.dy.GetManifestById(context.Background(), store.tableName, manifestId)
	if err != nil {
		message := "Error: Invalid ManifestID |||| Manifest ID: " + manifestId
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	// Check that manifest is not archived.
	if activeManifest.Status == manifest.Archived.String() {
		message := "Cannot sync with an 'archived' manifest. Archived manifests can be downloaded as a CSV file."
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
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

	//var mf *dbTable.ManifestFileTable
	manifestFiles, lastKey, err := store.dy.GetFilesPaginated(context.Background(), table, manifestId, status, limit, startKey)
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
// for the returned files is updated to "Verified". This enables the workflow for the agent to verify completed uploads
// and indicate that the uploads were verified by the client.
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
	//var mf *dbTable.ManifestFileTable
	files, lastKey, err := store.dy.GetFilesPaginated(context.Background(), store.fileTableName, manifestId, status, 500, startKey)
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
			err = store.dy.UpdateFileTableStatus(context.Background(), store.fileTableName, manifestId, f, manifestFile.Verified, "")
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

// postManifestArchiveRoute exports a manifest to S3 and removes files from manifestFileTable
func postManifestArchiveRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	apiResponse := events.APIGatewayV2HTTPResponse{}
	queryParams := request.QueryStringParameters

	var manifestId string
	var found bool
	if manifestId, found = queryParams["manifest_id"]; !found {
		message := "Error: ManifestID not specified"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	removeFiles, err := strconv.ParseBool(queryParams["remove"])
	if err != nil {
		message := "Error: Incorrectly specified 'remove' parameter"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	// Verify that manifestID belongs to DatasetID in Claim.
	// This is redundant as request is already authorized but no harm in checking twice.

	// Set Manifest to "archiving"
	eventData := ArchiveEvent{
		ManifestId:     manifestId,
		OrganizationId: claims.OrgClaim.IntId,
		DatasetId:      claims.DatasetClaim.IntId,
		RemoveFromDB:   removeFiles,
	}

	// Call ArchiveLambda in asynchronous way.
	payload, err := json.Marshal(eventData)
	if err != nil {
		return &events.APIGatewayV2HTTPResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	_, err = store.lambdaClient.Invoke(context.Background(),
		&lambda.InvokeInput{
			InvocationType: types2.InvocationTypeEvent,
			FunctionName:   aws.String(os.Getenv("ARCHIVER_INVOKE_ARN")),
			Payload:        payload,
		},
	)
	if err != nil {
		message := "Error: could not invoke manifest archiver workflow"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// Return Success Message
	responseBody := ArchivePostResponse{
		Message:    "Manifest archive workflow triggered.",
		ManifestId: manifestId,
	}
	jsonBody, _ := json.Marshal(responseBody)
	apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200}
	return &apiResponse, nil
}

// getManifestArchiveUrl returns a pre-signed url for downloading an archived manifest
func getManifestArchiveUrl(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	apiResponse := events.APIGatewayV2HTTPResponse{}
	queryParams := request.QueryStringParameters

	var manifestId string
	var found bool
	if manifestId, found = queryParams["manifest_id"]; !found {
		message := "Error: ManifestID not specified"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	manifestLocation := fmt.Sprintf("O%d/D%d/manifest_archive_%s.csv",
		claims.OrgClaim.IntId, claims.DatasetClaim.IntId, manifestId)

	log.WithFields(
		log.Fields{
			"manifest_id":     manifestId,
			"organization_id": claims.OrgClaim.IntId,
			"dataset_id":      claims.DatasetClaim.NodeId,
		}).Info(fmt.Sprintf("Getting Pre-signed url for: %s", manifestLocation))

	preSignClient := s3.NewPresignClient(store.s3Client)
	ctx := context.Background()
	preSignResult, err := preSignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(archiveBucket),
		Key:    aws.String(manifestLocation),
	},
		s3.WithPresignExpires(time.Minute*15),
	)

	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id":     manifestId,
				"organization_id": claims.OrgClaim.IntId,
				"dataset_id":      claims.DatasetClaim.NodeId,
			}).Error(fmt.Sprintf("Cannot create pre-signed url: %v", err))

		message := "Error: could not create pre-signed url for object"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	responseBody := ArchiveGetResponse{
		Message: "Navigating to this URL will download the manifest file.",
		Url:     preSignResult.URL,
	}

	jsonBody, _ := json.Marshal(responseBody)

	response := events.APIGatewayV2HTTPResponse{
		StatusCode: 200,
		Body:       string(jsonBody),
	}

	return &response, nil

}

// deleteManifestRoute removes manifest from manifest Table. Requires manifest to be archived previously.
func deleteManifestRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	apiResponse := events.APIGatewayV2HTTPResponse{}
	queryParams := request.QueryStringParameters

	var manifestId string
	var found bool
	if manifestId, found = queryParams["manifest_id"]; !found {
		message := "Error: ManifestID not specified"
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 400), StatusCode: 400}
		return &apiResponse, nil
	}

	ctx := context.Background()
	err := store.dy.DeleteManifest(ctx, store.tableName, manifestId)
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id":     manifestId,
				"organization_id": claims.OrgClaim.IntId,
				"dataset_id":      claims.DatasetClaim.NodeId,
			}).Error(err.Error())

		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(err.Error(), 500), StatusCode: 500}
		return &apiResponse, nil
	}

	responseBody := ArchiveDeleteResponse{Message: "Success"}
	jsonBody, _ := json.Marshal(responseBody)
	response := events.APIGatewayV2HTTPResponse{StatusCode: 200, Body: string(jsonBody)}
	return &response, nil

}
