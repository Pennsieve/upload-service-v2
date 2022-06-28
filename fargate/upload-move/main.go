package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	manifestPkg "github.com/pennsieve/pennsieve-go-api/pkg/manifest"
	"log"
	"os"
	"sync"
)

var manifestSession manifestPkg.ManifestSession
var uploadBucket string

type Item struct {
	ManifestId string `dynamodbav:"ManifestId"`
	UploadId   string `dynamodbav:"UploadId"`
}

type fileWalk chan Item

var processWg sync.WaitGroup

const nrWorkers = 20

// main entry method for the task.
func main() {

	// Initializing environment
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	manifestSession = manifestPkg.ManifestSession{
		FileTableName: os.Getenv("FILES_TABLE"),
		TableName:     os.Getenv("MANIFEST_TABLE"),
		Client:        dynamodb.NewFromConfig(cfg),
	}

	uploadBucket = os.Getenv("UPLOAD_BUCKET")

	walker := make(fileWalk)

	// Walk over all files in IMPORTED status and make available on channel for processors.
	go func() {
		defer func() {
			close(walker)
		}()

		// Get all the files in Uploaded State from Dynamodb and put on channel.
		if err := manifestFileWalk(walker); err != nil {
			log.Fatalf("Manifest File Walker failed: %v", err)
		}
	}()

	// Initiate the upload workers
	for w := 1; w <= nrWorkers; w++ {
		processWg.Add(1)
		log.Println("starting worker:", w)
		w := int32(w)
		go func() {
			err := moveFile(w, walker)
			if err != nil {
				log.Println("Error in Move Worker:", err)
			}
		}()
	}

	// Wait until all processors are completed.
	processWg.Wait()

	log.Println("Finished with task.")
}

// manifestFileWalk paginates results from dynamodb manifest files table and put items on channel.
func manifestFileWalk(walker fileWalk) error {

	p := dynamodb.NewQueryPaginator(manifestSession.Client, &dynamodb.QueryInput{
		TableName:              aws.String(manifestSession.FileTableName),
		IndexName:              aws.String("StatusIndex"),
		Limit:                  aws.Int32(5),
		KeyConditionExpression: aws.String("#status = :hashKey"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":hashKey": &types.AttributeValueMemberS{Value: "Imported"},
		},
		ExpressionAttributeNames: map[string]string{
			"#status": "Status",
		},
	})

	log.Println("In manifest file walk")

	for p.HasMorePages() {
		log.Println("Getting page from dynamodb")

		out, err := p.NextPage(context.TODO())
		if err != nil {
			panic(err)
		}

		var pItems []Item
		err = attributevalue.UnmarshalListOfMaps(out.Items, &pItems)
		if err != nil {
			panic(err)
		}

		// Add items to the channel
		for _, item := range pItems {
			walker <- item
		}

	}

	return nil
}

// moveFile accepts an item from the channel and implements the move workflow for that item.
func moveFile(workerId int32, items <-chan Item) error {

	defer func() {
		log.Println("Closing Worker: ", workerId)
		processWg.Done()
	}()

	for item := range items {
		log.Println(fmt.Sprintf("%d - %s", workerId, item.UploadId))

	}

	return nil
}
