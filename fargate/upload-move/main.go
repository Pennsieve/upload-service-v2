package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/pkg/core"
	manifestPkg "github.com/pennsieve/pennsieve-go-api/pkg/manifest"
	"log"
	"os"
	"sync"
)

var manifestSession manifestPkg.ManifestSession
var uploadBucket string
var defaultStorageBucket string

type Item struct {
	ManifestId string `dynamodbav:"ManifestId"`
	UploadId   string `dynamodbav:"UploadId"`
}

type fileWalk chan Item

var processWg sync.WaitGroup

const nrWorkers = 20

// storageBucketMap maps manifestIds to storageBucket names
var storageBucketMap map[string]string

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
	defaultStorageBucket = os.Getenv("STORAGE_BUCKET")

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

// getManifestStorageBucket returns the storage bucket associated with organization for manifest.
func getManifestStorageBucket(manifestId string) (string, error) {

	// If cached value exists, return cached value
	if val, ok := storageBucketMap[manifestId]; ok {
		return val, nil
	}

	// Get manifest from dynamodb based on id
	manifest, err := dbTable.GetFromManifest(manifestSession.Client, manifestSession.TableName, manifestId)

	// Get Organization associated with upload Manifest
	db, err := core.ConnectRDS()
	if err != nil {
		return "", err
	}

	var o dbTable.Organization
	org, err := o.Get(db, manifest.OrganizationId)
	if err != nil {
		log.Println("Error getting organization: ", err)
		return "", err
	}

	// Return storagebucket if defined, or default bucket.
	sbName := defaultStorageBucket
	if org.StorageBucket != "" {
		sbName = org.StorageBucket
	}

	storageBucketMap[manifestId] = sbName

	return sbName, nil
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

	// Close worker after it completes.
	// This happens when the items channel closes.
	defer func() {
		log.Println("Closing Worker: ", workerId)
		processWg.Done()
	}()

	// Iterate over items from the channel.
	for item := range items {

		targetBucket, err := getManifestStorageBucket(item.ManifestId)
		if err != nil {
			log.Println("Error getting storage bucket for manifest: ", err)
			return err
		}

		log.Println(fmt.Sprintf("%d - %s - %s", workerId, item.UploadId, targetBucket))

	}

	return nil
}
