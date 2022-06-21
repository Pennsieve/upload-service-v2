package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/manifest"
	"log"
	"sync"
	"time"
)

var syncWG sync.WaitGroup

const batchSize = 25 // maximum batch size for batchPut action on dynamodb
const nrWorkers = 2  // preliminary profiling shows that more workers don't improve efficiency for up to 1000 files

// fileWalk channel used to distribute FileDTOs to the workers importing the files in DynamoDB
type fileWalk chan manifest.FileDTO

// createManifest creates a new Manifest in DynamoDB
func createManifest(item dbTable.ManifestTable) error {

	data, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Printf("MarshalMap: %v\n", err)
		return fmt.Errorf("MarshalMap: %v\n", err)
	}

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
func addFiles(manifestId string, items []manifest.FileDTO) (*manifest.AddFilesStats, error) {

	walker := make(fileWalk, batchSize)
	result := make(chan manifest.AddFilesStats, nrWorkers)

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

	// Initiate a set of upload workers as go-routines
	for w := 1; w <= nrWorkers; w++ {
		w2 := int32(w)
		syncWG.Add(1)
		log.Println("starting worker:", w2)

		go func() {
			stats, _ := createOrUpdateFile(w2, walker, manifestId)
			result <- *stats
		}()
	}

	syncWG.Wait()
	close(result)
	fmt.Println("WAIT GROUP DONE")

	resp := manifest.AddFilesStats{}
	for r := range result {
		resp.NrFilesUpdated += r.NrFilesUpdated
		resp.NrFilesRemoved += r.NrFilesRemoved
		resp.FailedFiles = append(resp.FailedFiles, r.FailedFiles...)
	}

	return &resp, nil

}

// updateDynamoDb sends a set of FileDTOs to dynamodb.
func updateDynamoDb(manifestId string, fileSlice []manifest.FileDTO) (*manifest.AddFilesStats, error) {
	// Create Batch Put request for the fileslice and update dynamodb with one call
	var writeRequests []types.WriteRequest

	// Iterate over files in the fileSlice array and create writeRequests.
	var nrFilesUpdated int
	var nrFilesRemoved int
	var request types.WriteRequest
	for _, file := range fileSlice {

		switch file.Status {
		case manifest.FileRemoved:
			data, err := attributevalue.MarshalMap(dbTable.ManifestFilePrimaryKey{
				ManifestId: manifestId,
				UploadId:   file.UploadID,
			})
			if err != nil {
				log.Fatalf("MarshalMap: %v\n", err)
			}
			request = types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: data,
				},
			}
			nrFilesRemoved++
		case manifest.FileInitiated, manifest.FileFailed:
			item := dbTable.ManifestFileTable{
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
			request = types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: data,
				},
			}
			nrFilesUpdated++
		default:
			log.Fatalln("Unknown Type:", file.Status)
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
		log.Println("CONTAINS UNPROCESSED DATA", unProcessedItems)
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
		fileEntry := dbTable.ManifestFileTable{}
		err := attributevalue.UnmarshalMap(item, &fileEntry)
		if err != nil {
			fmt.Println("Unable to UnMarshall unprocessed items. ", err)
			return nil, err
		}
		failedFiles = append(failedFiles, fileEntry.UploadId)

		fmt.Println("STATUS ", fileEntry.Status)
		switch fileEntry.Status {
		case manifest.FileRemoved.String():
			nrFilesRemoved--
		case manifest.FileInitiated.String(), manifest.FileFailed.String():
			nrFilesUpdated--
		default:
			log.Fatalln("NO match")
		}
	}

	response := manifest.AddFilesStats{
		NrFilesUpdated: nrFilesUpdated,
		NrFilesRemoved: nrFilesRemoved,
		FailedFiles:    failedFiles,
	}
	return &response, err

}

// createOrUpdateFile is run in a goroutine and grabs set of files from channel and calls updateDynamoDb.
func createOrUpdateFile(workerId int32, files fileWalk, manifestId string) (*manifest.AddFilesStats, error) {
	defer func() {
		log.Println("Closing Worker: ", workerId)
		syncWG.Done()
	}()

	response := manifest.AddFilesStats{}

	// Create file slice of size "batchSize" or smaller if end of list.
	var fileSlice []manifest.FileDTO = nil
	for record := range files {
		fileSlice = append(fileSlice, record)

		// When the number of items in fileSize matches the batchSize --> make call to update dynamodb
		if len(fileSlice) == batchSize {
			stats, _ := updateDynamoDb(manifestId, fileSlice)
			fileSlice = nil

			response.NrFilesUpdated += stats.NrFilesUpdated
			response.NrFilesRemoved += stats.NrFilesRemoved
			response.FailedFiles = append(response.FailedFiles, stats.FailedFiles...)
		}
	}

	// Add final partially filled fileSlice to database
	if fileSlice != nil {
		stats, _ := updateDynamoDb(manifestId, fileSlice)
		response.NrFilesUpdated += stats.NrFilesUpdated
		response.NrFilesRemoved += stats.NrFilesRemoved
		response.FailedFiles = append(response.FailedFiles, stats.FailedFiles...)
	}

	return &response, nil
}
