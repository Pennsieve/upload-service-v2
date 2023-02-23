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
	"github.com/pennsieve/pennsieve-go-core/pkg/core"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dbTable"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload-move-files/pkg"
	log "github.com/sirupsen/logrus"
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
	datasetId      int64
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

	log.SetLevel(log.InfoLevel)

	// Initialize logger
	log.SetFormatter(&log.JSONFormatter{})

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
		log.Debug("starting worker:", w)
		w := int32(w)
		go func() {
			err := moveFile(w, walker)
			if err != nil {
				log.Error("Error in Move Worker:", err)
			}
		}()
	}

	// Wait until all processors are completed.
	processWg.Wait()

	log.Println("Finished with task.")
}

// getManifestStorageBucket returns the storage bucket associated with organization for manifest.
func getManifestStorageBucket(manifestId string) (*storageOrgItem, error) {

	var m *dbTable.ManifestTable

	// If cached value exists, return cached value
	if val, ok := storageBucketMap[manifestId]; ok {
		return &val, nil
	}

	// Get manifest from dynamodb based on id
	manifest, err := m.GetFromManifest(Session.DynamodbClient, Session.TableName, manifestId)

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

	// Return storagebucket if defined, or default bucket.
	sbName := defaultStorageBucket
	if org.StorageBucket.Valid {
		sbName = org.StorageBucket.String
	}

	si := storageOrgItem{
		organizationId: manifest.OrganizationId,
		storageBucket:  sbName,
		datasetId:      manifest.DatasetId,
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
		log.Debug("Closing Worker: ", workerId)
		processWg.Done()
	}()

	// Iterate over items from the channel.
	for item := range items {

		var mf *dbTable.ManifestFileTable

		// This check should be obsolete but want to add a double check to ensure we never remove files that have not
		// been successfully copied to final location.
		moveSuccess := false

		stOrgItem, err := getManifestStorageBucket(item.ManifestId)
		if err != nil {
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Error("Error getting storage bucket for manifest: ", err)
			return err
		}

		log.Debug(fmt.Sprintf("%d - %s - %s", workerId, item.UploadId, stOrgItem.storageBucket))

		sourceKey := fmt.Sprintf("%s/%s", item.ManifestId, item.UploadId)
		sourcePath := fmt.Sprintf("%s/%s/%s", uploadBucket, item.ManifestId, item.UploadId)
		targetPath := fmt.Sprintf("O%d/D%d/%s/%s", stOrgItem.organizationId, stOrgItem.datasetId, item.ManifestId, item.UploadId)

		// Get File Size
		headObj := s3.HeadObjectInput{
			Bucket: aws.String(uploadBucket),
			Key:    aws.String(sourceKey),
		}
		result, err := Session.S3Client.HeadObject(context.Background(), &headObj)
		if err != nil {
			log.WithFields(
				log.Fields{
					"upload_bucket": uploadBucket,
					"s3_key":        sourceKey,
				}).Error("moveFile: Cannot get size of S3 object.")
			err = mf.UpdateFileTableStatus(Session.DynamodbClient, Session.FileTableName, item.ManifestId, item.UploadId, manifestFile.Failed, err.Error())
			if err != nil {
				log.Println("Error updating Dynamodb status: ", err)
				continue
			}
			continue
		}

		// Copy File
		fileSize := result.ContentLength           // size in bytes
		const maxFileSize = 5 * 1000 * 1000 * 1000 // 5GiB (real limit is 5GB but want to be conservative)
		if fileSize < maxFileSize {
			err = simpleCopyFile(stOrgItem, sourcePath, targetPath)
			if err != nil {
				log.Error(fmt.Sprintf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err))
				err = mf.UpdateFileTableStatus(Session.DynamodbClient, Session.FileTableName, item.ManifestId, item.UploadId, manifestFile.Failed, err.Error())
				if err != nil {
					log.Error("Error updating Dynamodb status: ", err)
					continue
				}
				continue
			} else {
				moveSuccess = true
			}
		} else {
			err = pkg.MultiPartCopy(Session.S3Client, fileSize, uploadBucket, sourceKey, stOrgItem.storageBucket, targetPath)
			if err != nil {
				log.Error(fmt.Sprintf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err))
				err = mf.UpdateFileTableStatus(Session.DynamodbClient, Session.FileTableName, item.ManifestId, item.UploadId, manifestFile.Failed, err.Error())
				if err != nil {
					log.Error("Error updating Dynamodb status: ", err)
					continue
				}
				continue
			} else {
				moveSuccess = true
			}
		}

		log.WithFields(
			log.Fields{
				"manifest_id": item.ManifestId,
				"upload_id":   item.UploadId,
				"s3_target":   targetPath,
			}).Infof("%s copied to storage bin.", item.UploadId)

		updatedStatus := manifestFile.Finalized
		updatedMessage := ""
		var f dbTable.File

		switch err := f.UpdateBucket(Session.pgClient, item.UploadId, stOrgItem.storageBucket, targetPath, stOrgItem.organizationId); err.(type) {
		case nil:
			break
		case *dbTable.ErrFileNotFound:
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Info(err.Error())

			updatedStatus = manifestFile.Failed
			updatedMessage = err.Error()

		case *dbTable.ErrMultipleRowsAffected:
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Error(err.Error())

			updatedStatus = manifestFile.Failed
			updatedMessage = err.Error()

		default:
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Error(err.Error())

			updatedStatus = manifestFile.Failed
			updatedMessage = err.Error()
			moveSuccess = false
		}

		// Deleting item in Uploads Folder if successfully moved to final location.
		if moveSuccess == true {
			deleteParams := s3.DeleteObjectInput{
				Bucket: aws.String(uploadBucket),
				Key:    aws.String(fmt.Sprintf("%s/%s", item.ManifestId, item.UploadId)),
			}
			_, err = Session.S3Client.DeleteObject(context.Background(), &deleteParams)
			if err != nil {
				log.WithFields(
					log.Fields{
						"manifest_id": item.ManifestId,
						"upload_id":   item.UploadId,
					}).Error("Unable to delete file.")
				continue
			}
		}

		// Update status of files in dynamoDB
		err = mf.UpdateFileTableStatus(Session.DynamodbClient, Session.FileTableName, item.ManifestId, item.UploadId, updatedStatus, updatedMessage)
		if err != nil {
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Error("Error updating Dynamodb status: ", err)
		}

	}

	return nil
}

func simpleCopyFile(stOrgItem *storageOrgItem, sourcePath string, targetPath string) error {
	// Copy the item

	log.Debug("Simple copy: ", sourcePath, " to: ", stOrgItem.storageBucket, ":", targetPath)

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
