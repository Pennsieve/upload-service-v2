package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/manifest"
	"github.com/pennsieve/pennsieve-go-api/pkg/core"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var session awsSession
var uploadBucket string
var defaultStorageBucket string

type Item struct {
	ManifestId string `dynamodbav:"ManifestId"`
	UploadId   string `dynamodbav:"UploadId"`
}

type storageOrgItem struct {
	organizationId int64
	storageBucket  string
}

type fileWalk chan Item

var processWg sync.WaitGroup

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

	session = awsSession{
		FileTableName:  os.Getenv("FILES_TABLE"),
		TableName:      os.Getenv("MANIFEST_TABLE"),
		DynamodbClient: dynamodb.NewFromConfig(cfg),
		S3Client:       s3.NewFromConfig(cfg),
	}

	// Get Postgres connection
	db, err := core.ConnectRDS()
	session.pgClient = db
	if err != nil {
		log.Fatalf("Cannot connect to the Pennsieve Postgres Proxy.")
	}
	defer session.pgClient.Close()

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
	manifest, err := dbTable.GetFromManifest(session.DynamodbClient, session.TableName, manifestId)

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

	p := dynamodb.NewQueryPaginator(session.DynamodbClient, &dynamodb.QueryInput{
		TableName:              aws.String(session.FileTableName),
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

		// TODO: Check if we need to multi-part copy

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
		result, err := session.S3Client.HeadObject(context.Background(), &headObj)
		if err != nil {
			log.Println("moveFile: Cannot get size of S3 object.")
			continue
		}

		// Copy File
		fileSize := result.ContentLength           // size in bytes
		const maxFileSize = 5 * 1000 * 1000 * 1000 // 5GiB (real limit is 5GB but want to be conservative)
		//if fileSize < maxFileSize {
		if false {
			log.Println("Simple copy")
			err = simpleCopyFile(stOrgItem, sourcePath, targetPath)
			if err != nil {
				log.Printf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err)
				continue
			}
		} else {
			log.Println("Multipart copy")
			err = MultiPartCopy(session.S3Client, fileSize, uploadBucket, sourceKey, stOrgItem.storageBucket, targetPath)
			if err != nil {
				log.Printf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err)
				continue
			}
		}

		fmt.Printf("Item %q successfully copied from %s to  %s\n", item, sourcePath, targetPath)

		var f dbTable.File
		err = f.UpdateBucket(session.pgClient, item.UploadId, stOrgItem.storageBucket, stOrgItem.organizationId)
		if err != nil {
			log.Println("Could not update the bucket for ", item.UploadId)
			continue
		}

		// Update status of files in dynamoDB
		err = dbTable.UpdateFileTableStatus(session.DynamodbClient, session.FileTableName, item.ManifestId, item.UploadId, manifest.FileFinalized)
		if err != nil {
			log.Println("Error updating Dynamodb status: ", err)
			continue
		}

		// Deleting item in Uploads Folder if successfully moved to final location.
		deleteParams := s3.DeleteObjectInput{
			Bucket: aws.String(uploadBucket),
			Key:    aws.String(fmt.Sprintf("%s/%s", item.ManifestId, item.UploadId)),
		}
		_, err = session.S3Client.DeleteObject(context.Background(), &deleteParams)
		if err != nil {
			log.Printf("Unable to delete file: %s/%s\n", item.ManifestId, item.UploadId)
			continue
		}

	}

	return nil
}

func simpleCopyFile(stOrgItem *storageOrgItem, sourcePath string, targetPath string) error {
	// Copy the item
	params := s3.CopyObjectInput{
		Bucket:     aws.String(stOrgItem.storageBucket),
		CopySource: aws.String(sourcePath),
		Key:        aws.String(targetPath),
	}

	_, err := session.S3Client.CopyObject(context.Background(), &params)
	if err != nil {
		return err
	}

	return nil
}

// maxPartSize constant for number of bits in 50 megabyte chunk
// this corresponds with max file size of 500GB per file as copy can do max 10,000 parts.
const maxPartSize = 50 * 1024 * 1024

//helper function to build the string for the range of bits to copy
func buildCopySourceRange(start int64, objectSize int64) string {
	end := start + maxPartSize - 1
	if end > objectSize {
		end = objectSize - 1
	}
	startRange := strconv.FormatInt(start, 10)
	stopRange := strconv.FormatInt(end, 10)
	return "bytes=" + startRange + "-" + stopRange
}

//function that starts, perform each part upload, and completes the copy
func MultiPartCopy(svc *s3.Client, fileSize int64, sourceBucket string, sourceKey string, destBucket string, destKey string) error {

	ctx, cancelFn := context.WithTimeout(context.TODO(), 30*time.Minute)
	defer cancelFn()

	//struct for starting a multipart upload
	startInput := s3.CreateMultipartUploadInput{
		Bucket: &destBucket,
		Key:    &destKey,
	}

	//send command to start copy and get the upload id as it is needed later
	var uploadId string
	createOutput, err := svc.CreateMultipartUpload(ctx, &startInput)
	if err != nil {
		return err
	}
	if createOutput != nil {
		if createOutput.UploadId != nil {
			uploadId = *createOutput.UploadId
		}
	}
	if uploadId == "" {
		return errors.New("no upload id found in start upload request")
	}

	var i int64
	var partNumber int32 = 1
	copySource := "/" + sourceBucket + "/" + sourceKey
	parts := make([]s3types.CompletedPart, 0)
	numUploads := fileSize / maxPartSize
	log.Printf("Will attempt upload in %d number of parts to %s\n", numUploads, destKey)
	for i = 0; i < fileSize; i += maxPartSize {
		copyRange := buildCopySourceRange(i, fileSize)
		partInput := s3.UploadPartCopyInput{
			Bucket:          &destBucket,
			CopySource:      &copySource,
			CopySourceRange: &copyRange,
			Key:             &destKey,
			PartNumber:      partNumber,
			UploadId:        &uploadId,
		}
		log.Printf("Attempting to upload part %d range: %s\n", partNumber, copyRange)
		partResp, err := svc.UploadPartCopy(context.TODO(), &partInput)

		if err != nil {
			log.Println("Attempting to abort upload")
			abortIn := s3.AbortMultipartUploadInput{
				UploadId: &uploadId,
			}
			//ignoring any errors with aborting the copy
			svc.AbortMultipartUpload(context.TODO(), &abortIn)
			return fmt.Errorf("error uploading part %d : %w", partNumber, err)
		}

		//copy etag and part number from response as it is needed for completion
		if partResp != nil {
			partNum := partNumber
			etag := strings.Trim(*partResp.CopyPartResult.ETag, "\"")
			cPart := s3types.CompletedPart{
				ETag:       &etag,
				PartNumber: partNum,
			}
			parts = append(parts, cPart)
			log.Printf("Successfully upload part %d of %s\n", partNumber, uploadId)
		}
		partNumber++
		if partNumber%50 == 0 {
			log.Printf("Completed part %d of %d to %s\n", partNumber, numUploads, destKey)
		}
	}

	//create struct for completing the upload
	mpu := s3types.CompletedMultipartUpload{
		Parts: parts,
	}

	//complete actual upload
	//does not actually copy if the complete command is not received
	complete := s3.CompleteMultipartUploadInput{
		Bucket:          &destBucket,
		Key:             &destKey,
		UploadId:        &uploadId,
		MultipartUpload: &mpu,
	}
	compOutput, err := svc.CompleteMultipartUpload(context.TODO(), &complete)
	if err != nil {
		return fmt.Errorf("error completing upload: %w", err)
	}
	if compOutput != nil {
		log.Printf("Successfully copied Bucket: %s Key: %s to Bucket: %s Key: %s\n", sourceBucket, sourceKey, destBucket, destKey)
	}
	return nil
}
