package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-api/models/manifest"
	"log"
	"sync"
	"time"
)

var syncWG sync.WaitGroup

const batchSize = 25 // maximum batch size for batchPut action on dynamodb
const nrWorkers = 2  // preliminary profiling shows that more workers don't improve efficiency for up to 1000 files

// manifestTable is a representation of a Manifest in DynamoDB
type manifestTable struct {
	ManifestId string `dynamodbav:"ManifestId"`
	DatasetId  string `dynamodbav:"DatasetId"`
	UserId     int64  `dynamodbav:"UserId"`
	Status     string `dynamodbav:"Status"`
}

// manifestFileTable is a representation of a ManifestFile in DynamoDB
type manifestFileTable struct {
	ManifestId string `dynamodbav:"ManifestId"`
	UploadId   string `dynamodbav:"UploadId"`
	FilePath   string `dynamodbav:"FilePath,omitempty"`
	FileName   string `dynamodbav:"FileName"`
	Status     string `dynamodbav:"Status"`
}

// fileWalk channel used to distribute FileDTOs to the workers importing the files in DynamoDB
type fileWalk chan manifest.FileDTO

// addFilesStats object that is returned to the client.
type addFilesStats struct {
	nrFilesAdded int
	failedFiles  []string
}

// getFromManifest returns a Manifest item for a given manifest ID.
func getFromManifest(manifestId string) (*manifestTable, error) {

	item := manifestTable{}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("LoadDefaultConfig: %v\n", err)
	}

	// Create an Amazon DynamoDB client.
	client := dynamodb.NewFromConfig(cfg)

	data, err := client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(manifestTableName),
		Key: map[string]types.AttributeValue{
			"ManifestId": &types.AttributeValueMemberS{Value: manifestId},
		},
	})

	if err != nil {
		return &item, fmt.Errorf("GetItem: %v\n", err)
	}

	if data.Item == nil {
		return &item, fmt.Errorf("GetItem: Manifest not found.\n")
	}

	err = attributevalue.UnmarshalMap(data.Item, &item)
	if err != nil {
		return &item, fmt.Errorf("UnmarshalMap: %v\n", err)
	}

	return &item, nil

}

// createManifest creates a new Manifest in DynamoDB
func createManifest(item manifestTable) error {

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Printf("LoadDefaultConfig: %v\n\n", err)
		return fmt.Errorf("LoadDefaultConfig: %v\n", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	data, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Printf("MarshalMap: %v\n", err)
		return fmt.Errorf("MarshalMap: %v\n", err)
	}

	fmt.Println(data)

	_, err = client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(manifestTableName),
		Item:      data,
	})

	if err != nil {
		log.Printf("PutItem: %v\n", err)
		return fmt.Errorf("PutItem: %v\n", err)
	}

	return nil
}

// addFiles manages the workers and defines the go routines to add files to manifest db.
func addFiles(manifestId string, items []manifest.FileDTO) (*addFilesStats, error) {

	walker := make(fileWalk, batchSize)
	result := make(chan addFilesStats, nrWorkers)

	// List crawler
	go func() {
		// Gather the files to upload by walking the path recursively
		defer func() {
			close(walker)
		}()
		fmt.Println("NUMBER OF ITEMS:", len(items))

		for _, f := range items {
			walker <- f
		}
	}()

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	// Initiate a set of upload workers as go-routines
	for w := 1; w <= nrWorkers; w++ {
		w2 := int32(w)
		syncWG.Add(1)
		log.Println("starting worker:", w2)

		go func() {
			stats, _ := createOrUpdateFile(client, w2, walker, manifestId)
			result <- *stats
		}()
	}

	syncWG.Wait()
	close(result)
	fmt.Println("WAIT GROUP DONE")

	resp := addFilesStats{
		nrFilesAdded: 0,
		failedFiles:  nil,
	}

	for r := range result {
		resp.nrFilesAdded += r.nrFilesAdded
		resp.failedFiles = append(resp.failedFiles, r.failedFiles...)
	}

	return &resp, nil

}

// updateDynamoDb sends a set of FileDTOs to dynamodb.
func updateDynamoDb(client *dynamodb.Client, manifestId string, fileSlice []manifest.FileDTO) (*addFilesStats, error) {
	// Create Batch Put request for the fileslice and update dynamodb with one call
	var writeRequests []types.WriteRequest

	// Iterate over files in the fileSlice array and create writeRequests.
	for _, file := range fileSlice {

		item := manifestFileTable{
			ManifestId: manifestId,
			UploadId:   file.UploadID,
			FilePath:   file.TargetPath,
			FileName:   file.TargetName,
			Status:     manifest.FileSynced.String(),
		}

		data, err := attributevalue.MarshalMap(item)
		if err != nil {
			log.Fatalf("MarshalMap: %v\n", err)
		}

		request := types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: data,
			},
		}

		writeRequests = append(writeRequests, request)
	}

	// Format requests and call DynamoDB
	requestItems := map[string][]types.WriteRequest{
		manifestFileTableName: writeRequests,
	}

	params := dynamodb.BatchWriteItemInput{
		RequestItems:                requestItems,
		ReturnConsumedCapacity:      "NONE",
		ReturnItemCollectionMetrics: "NONE",
	}

	data, err := client.BatchWriteItem(context.Background(), &params)
	if err != nil {
		log.Fatalln("Unable to Batch Write: ", err)
	}

	// Handle potential failed files:
	// Step 1: Retry if there are unprocessed files.
	nrRetries := 3
	retryIndex := 0
	unProcessedItems := data.UnprocessedItems
	for len(unProcessedItems) > 0 {
		params := dynamodb.BatchWriteItemInput{
			RequestItems:                unProcessedItems,
			ReturnConsumedCapacity:      "NONE",
			ReturnItemCollectionMetrics: "NONE",
		}

		data, err = client.BatchWriteItem(context.Background(), &params)
		unProcessedItems = data.UnprocessedItems

		retryIndex++
		if retryIndex == nrRetries {
			fmt.Printf("Dynamodb did not ingest all the file records.")
			break
		}
		time.Sleep(time.Duration(100*retryIndex) * time.Millisecond)
	}

	// Step 2: Set the failedFiles array to return failed update to client.
	var failedFiles []string
	putRequestList := unProcessedItems[manifestFileTableName]
	for _, f := range putRequestList {
		item := f.PutRequest.Item
		fileEntry := manifestFileTable{}
		err := attributevalue.UnmarshalMap(item, fileEntry)
		if err != nil {
			fmt.Println("Unable to UnMarshall unprocessed items. ", err)
			return nil, err
		}
		failedFiles = append(failedFiles, fileEntry.UploadId)
	}

	response := addFilesStats{
		nrFilesAdded: len(fileSlice) - len(failedFiles),
		failedFiles:  failedFiles,
	}
	return &response, err

}

// createOrUpdateFile is run in a goroutine and grabs set of files from channel and calls updateDynamoDb.
func createOrUpdateFile(client *dynamodb.Client, workerId int32, files fileWalk, manifestId string) (*addFilesStats, error) {
	defer func() {
		log.Println("Closing Worker: ", workerId)
		syncWG.Done()
	}()

	response := addFilesStats{}

	// Create file slice of size "batchSize" or smaller if end of list.
	var fileSlice []manifest.FileDTO = nil
	for record := range files {
		fileSlice = append(fileSlice, record)

		// When the number of items in fileSize matches the batchSize --> make call to update dynamodb
		if len(fileSlice) == batchSize {
			stats, _ := updateDynamoDb(client, manifestId, fileSlice)
			fileSlice = nil

			response.nrFilesAdded += stats.nrFilesAdded
			response.failedFiles = append(response.failedFiles, stats.failedFiles...)
		}
	}

	// Add final partially filled fileSlice to database
	if fileSlice != nil {
		stats, _ := updateDynamoDb(client, manifestId, fileSlice)
		response.nrFilesAdded += stats.nrFilesAdded
		response.failedFiles = append(response.failedFiles, stats.failedFiles...)
	}

	return &response, nil
}
