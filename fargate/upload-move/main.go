package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-api/pkg/core"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload-move-files/pkg"
	"log"
	"os"
	"sync"
)

var Session awsSession
var uploadBucket string
var defaultStorageBucket string

type Item struct {
	ManifestId string `dynamodbav:"ManifestId"`
	UploadId   string `dynamodbav:"UploadId"`
	Status     string `dynamodbav:"Status"`
}

type storageOrgItem struct {
	organizationId int64
	storageBucket  string
}

type fileWalk chan Item

var processWg sync.WaitGroup

// Number of simultaneous copy threads.
const nrWorkers = 20

// storageBucketMap maps manifestIds to storageBucket names
var storageBucketMap = map[string]storageOrgItem{}

type awsSession struct {
	FileTableName  string
	TableName      string
	DynamodbClient *dynamodb.Client
	S3Client       *s3.Client
	pgClient       *sql.DB
}

// main entry method for the task.
func main() {

	// Initializing environment
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	Session = awsSession{
		FileTableName:  os.Getenv("FILES_TABLE"),
		TableName:      os.Getenv("MANIFEST_TABLE"),
		DynamodbClient: dynamodb.NewFromConfig(cfg),
		S3Client:       s3.NewFromConfig(cfg),
	}

	// Get Postgres connection
	db, err := core.ConnectRDS()
	Session.pgClient = db
	if err != nil {
		log.Fatalf("Cannot connect to the Pennsieve Postgres Proxy.")
	}
	defer Session.pgClient.Close()

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
func getManifestStorageBucket(manifestId string) (*storageOrgItem, error) {

	// If cached value exists, return cached value
	if val, ok := storageBucketMap[manifestId]; ok {
		return &val, nil
	}

	// Get manifest from dynamodb based on id
	manifest, err := dbTable.GetFromManifest(Session.DynamodbClient, Session.TableName, manifestId)

	// Get Organization associated with upload Manifest
	db, err := core.ConnectRDS()
	if err != nil {
		return nil, err
	}

	var o dbTable.Organization
	org, err := o.Get(db, manifest.OrganizationId)
	if err != nil {
		log.Println("Error getting organization: ", err)
		return nil, err
	}

	log.Println(org)

	// Return storagebucket if defined, or default bucket.
	sbName := defaultStorageBucket
	if org.StorageBucket.Valid {
		sbName = org.StorageBucket.String
	}

	si := storageOrgItem{
		organizationId: manifest.OrganizationId,
		storageBucket:  sbName,
	}

	storageBucketMap[manifestId] = si

	return &si, nil
}

// manifestFileWalk paginates results from dynamodb manifest files table and put items on channel.
func manifestFileWalk(walker fileWalk) error {

	p := dynamodb.NewQueryPaginator(Session.DynamodbClient, &dynamodb.QueryInput{
		TableName:              aws.String(Session.FileTableName),
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

		stOrgItem, err := getManifestStorageBucket(item.ManifestId)
		if err != nil {
			log.Println("Error getting storage bucket for manifest: ", err)
			return err
		}

		log.Println(fmt.Sprintf("%d - %s - %s", workerId, item.UploadId, stOrgItem.storageBucket))

		sourceKey := fmt.Sprintf("%s/%s", item.ManifestId, item.UploadId)
		sourcePath := fmt.Sprintf("%s/%s/%s", uploadBucket, item.ManifestId, item.UploadId)
		targetPath := fmt.Sprintf("%s/%s", item.ManifestId, item.UploadId)

		// Get File Size
		headObj := s3.HeadObjectInput{
			Bucket: aws.String(uploadBucket),
			Key:    aws.String(sourceKey),
		}
		result, err := Session.S3Client.HeadObject(context.Background(), &headObj)
		if err != nil {
			log.Println("moveFile: Cannot get size of S3 object.")
			continue
		}

		// Copy File
		fileSize := result.ContentLength           // size in bytes
		const maxFileSize = 5 * 1000 * 1000 * 1000 // 5GiB (real limit is 5GB but want to be conservative)
		if fileSize < maxFileSize {
			log.Println("Simple copy")
			err = simpleCopyFile(stOrgItem, sourcePath, targetPath)
			if err != nil {
				log.Printf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err)
				continue
			}
		} else {
			log.Println("Multipart copy")
			err = pkg.MultiPartCopy(Session.S3Client, fileSize, uploadBucket, sourceKey, stOrgItem.storageBucket, targetPath)
			if err != nil {
				log.Printf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err)
				continue
			}
		}

		fmt.Printf("Item %q successfully copied from %s to  %s\n", item, sourcePath, targetPath)

		var f dbTable.File
		err = f.UpdateBucket(Session.pgClient, item.UploadId, stOrgItem.storageBucket, stOrgItem.organizationId)
		if err != nil {
			log.Println("Could not update the bucket for ", item.UploadId)
			continue
		}

		// Update status of files in dynamoDB
		err = dbTable.UpdateFileTableStatus(Session.DynamodbClient, Session.FileTableName, item.ManifestId, item.UploadId, manifestFile.Finalized)
		if err != nil {
			log.Println("Error updating Dynamodb status: ", err)
			continue
		}

		// Deleting item in Uploads Folder if successfully moved to final location.
		deleteParams := s3.DeleteObjectInput{
			Bucket: aws.String(uploadBucket),
			Key:    aws.String(fmt.Sprintf("%s/%s", item.ManifestId, item.UploadId)),
		}
		_, err = Session.S3Client.DeleteObject(context.Background(), &deleteParams)
		if err != nil {
			log.Printf("Unable to delete file: %s/%s\n", item.ManifestId, item.UploadId)
			continue
		}

	}

	return nil
}

func simpleCopyFile(stOrgItem *storageOrgItem, sourcePath string, targetPath string) error {
	// Copy the item

	log.Println("Simple copy: ", sourcePath, " to: ", stOrgItem.storageBucket, ":", targetPath)

	params := s3.CopyObjectInput{
		Bucket:     aws.String(stOrgItem.storageBucket),
		CopySource: aws.String(sourcePath),
		Key:        aws.String(targetPath),
	}

	_, err := Session.S3Client.CopyObject(context.Background(), &params)
	if err != nil {
		return err
	}

	return nil
}
