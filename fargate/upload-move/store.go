package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	pgQeuries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload-move-files/pkg"
	log "github.com/sirupsen/logrus"
	"time"
)

// UploadMoveStore provides the Queries interface and a db instance.
type UploadMoveStore struct {
	pg   *pgQeuries.Queries
	dy   *dyQueries.Queries
	db   *sql.DB
	dydb *dynamodb.Client
	s3   *s3.Client
}

// NewUploadMoveStore returns a NewUploadMoveStore object which implements the Queries
func NewUploadMoveStore(db *sql.DB, dydb *dynamodb.Client, s3 *s3.Client) *UploadMoveStore {
	return &UploadMoveStore{
		db:   db,
		dydb: dydb,
		pg:   pgQeuries.New(db),
		dy:   dyQueries.New(dydb),
		s3:   s3,
	}
}

// execPgTx wrap function in transaction.
func (s *UploadMoveStore) execPgTx(ctx context.Context, fn func(*pgQeuries.Queries) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := pgQeuries.New(tx)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

func (s *UploadMoveStore) KeepAlive(ctx context.Context, ticker *time.Ticker) {
	for range ticker.C {
		_, err := s.db.QueryContext(ctx, "SELECT 1 as value FROM (VALUES(1)) i")
		if err != nil {
			log.Error(fmt.Sprintf("KeepAlive query failed: %v", err))
			db, err := pgQeuries.ConnectRDS()
			if err != nil {
				log.Error(fmt.Sprintf("KeepAlive ConnectRDS failed: %v", err))
			} else {
				// replace the database connection
				s.db = db
			}
		}
	}
}

// GetManifestStorageBucket returns the storage bucket associated with organization for manifest.
func (s *UploadMoveStore) GetManifestStorageBucket(manifestId string) (*storageOrgItem, error) {

	//var m *dbTable.ManifestTable

	// If cached value exists, return cached value
	if val, ok := storageBucketMap[manifestId]; ok {
		return &val, nil
	}

	// Get manifest from dynamodb based on id
	manifest, err := s.dy.GetManifestById(context.Background(), TableName, manifestId)
	if err != nil {
		err := fmt.Errorf("error getting manifest %s: %w", manifestId, err)
		return nil, err
	}

	//var o dbTable.Organization
	org, err := s.pg.GetOrganization(context.Background(), manifest.OrganizationId)
	if err != nil {
		err := fmt.Errorf("error getting organization %d referenced in manifest %s: %w",
			manifest.OrganizationId,
			manifestId,
			err)
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
func (s *UploadMoveStore) manifestFileWalk(walker fileWalk) error {

	p := dynamodb.NewQueryPaginator(s.dydb, &dynamodb.QueryInput{
		TableName:              aws.String(FileTableName),
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

	log.Debug("In manifest file walk")
	var pageNumber int
	for p.HasMorePages() {
		log.Debug("Getting page from dynamodb")

		out, err := p.NextPage(context.TODO())
		if err != nil {
			log.Error("error getting next page of dynamodb query results", err)
			continue
		}

		var pItems []Item
		err = attributevalue.UnmarshalListOfMaps(out.Items, &pItems)
		if err != nil {
			log.WithFields(log.Fields{"raw_items": out.Items}).Error("error parsing dynamodb query result page", err)
			continue
		}

		// Add items to the channel
		for _, item := range pItems {
			walker <- item
		}
		log.WithFields(log.Fields{"page_number": pageNumber, "item_count": out.Count}).Debug("added items to channel")
		pageNumber++
	}

	return nil
}

// moveFile accepts an item from the channel and implements the move workflow for that item.
func (s *UploadMoveStore) moveFile(workerId int, timeout time.Duration, items <-chan Item) {

	// Close worker after it completes.
	// This happens when the items channel closes.
	defer func() {
		log.Debug("Closing Worker: ", workerId)
		processWg.Done()
	}()

	// Iterate over items from the channel.
	for item := range items {

		//var mf *dbTable.ManifestFileTable

		// This check should be obsolete but want to add a double check to ensure we never remove files that have not
		// been successfully copied to final location.
		moveSuccess := false

		stOrgItem, err := s.GetManifestStorageBucket(item.ManifestId)
		if err != nil {
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Errorf("Error getting storage bucket for manifest: %v", err)
			continue
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
		result, err := s.s3.HeadObject(context.Background(), &headObj)
		if err != nil {
			log.WithFields(
				log.Fields{
					"upload_bucket": uploadBucket,
					"s3_key":        sourceKey,
				}).Error("moveFile: Cannot get size of S3 object.")
			err = s.dy.UpdateFileTableStatus(context.Background(), FileTableName, item.ManifestId, item.UploadId, manifestFile.Failed, err.Error())
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
			err = s.simpleCopyFile(stOrgItem, sourcePath, targetPath)
			if err != nil {
				log.Error(fmt.Sprintf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err))
				err = s.dy.UpdateFileTableStatus(context.Background(), FileTableName, item.ManifestId, item.UploadId, manifestFile.Failed, err.Error())
				if err != nil {
					log.Error("Error updating Dynamodb status: ", err)
					continue
				}
				continue
			} else {
				moveSuccess = true
			}
		} else {
			err = pkg.MultiPartCopy(s.s3, timeout, fileSize, uploadBucket, sourceKey, stOrgItem.storageBucket, targetPath)
			if err != nil {
				log.Error(fmt.Sprintf("Unable to copy item from  %s to %s, %v\n", sourcePath, targetPath, err))
				err = s.dy.UpdateFileTableStatus(context.Background(), FileTableName, item.ManifestId, item.UploadId, manifestFile.Failed, err.Error())
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
		//var f dbTable.File

		switch err := s.pg.UpdateBucketForFile(context.Background(), item.UploadId, stOrgItem.storageBucket, targetPath, stOrgItem.organizationId); err.(type) {
		case nil:
			break
		case *pgdb.ErrFileNotFound:
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Info(err.Error())

			updatedStatus = manifestFile.Failed
			updatedMessage = err.Error()

		case *pgdb.ErrMultipleRowsAffected:
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
			_, err = s.s3.DeleteObject(context.Background(), &deleteParams)
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
		err = s.dy.UpdateFileTableStatus(context.Background(), FileTableName, item.ManifestId, item.UploadId, updatedStatus, updatedMessage)
		if err != nil {
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Error("Error updating Dynamodb status: ", err)
		}

	}
}

// simpleCopyFile copy files between buckets using simple file copy method.
func (s *UploadMoveStore) simpleCopyFile(stOrgItem *storageOrgItem, sourcePath string, targetPath string) error {
	// Copy the item

	log.Debug("Simple copy: ", sourcePath, " to: ", stOrgItem.storageBucket, ":", targetPath)

	params := s3.CopyObjectInput{
		Bucket:     aws.String(stOrgItem.storageBucket),
		CopySource: aws.String(sourcePath),
		Key:        aws.String(targetPath),
	}

	_, err := s.s3.CopyObject(context.Background(), &params)
	if err != nil {
		return err
	}

	return nil
}
