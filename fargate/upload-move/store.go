package main

import (
	"context"
	"errors"
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
	"github.com/pennsieve/pennsieve-upload-service-v2/upload-move-files/pkg/pgmanager"
	log "github.com/sirupsen/logrus"
	"time"
)

// UploadMoveStore provides the Queries interface and a db instance.
type UploadMoveStore struct {
	pgManager           *pgmanager.PgManager
	dy                  *dyQueries.Queries
	dydb                *dynamodb.Client
	s3                  *s3.Client
	storageOrgItemCache *StorageOrgItemCache
}

// NewUploadMoveStore returns a NewUploadMoveStore object which implements the Queries
func NewUploadMoveStore(pgManager *pgmanager.PgManager, dydb *dynamodb.Client, s3 *s3.Client) *UploadMoveStore {
	return &UploadMoveStore{
		pgManager:           pgManager,
		dydb:                dydb,
		dy:                  dyQueries.New(dydb),
		s3:                  s3,
		storageOrgItemCache: NewStorageItemCache(DefaultStorageOrgItemQuery),
	}
}

// execPgTx wrap function in transaction.
func (s *UploadMoveStore) execPgTx(ctx context.Context, fn func(*pgQeuries.Queries) error) error {
	db, err := s.pgManager.DB()
	if err != nil {
		return fmt.Errorf("error accessing DB for TX execution: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
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

// GetManifestStorageBucket returns the storage bucket associated with organization for manifest.
func (s *UploadMoveStore) GetManifestStorageBucket(manifestId string) (*storageOrgItem, error) {
	pg, err := s.pgManager.Queries()
	if err != nil {
		return nil, fmt.Errorf("error getting pg queries for storageOrgItem lookup: %w", err)
	}
	return s.storageOrgItemCache.GetOrLoad(manifestId, s.dy, pg)
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
				}).Error("moveFile: Cannot get size of S3 object: ", err)
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

		pg, err := s.pgManager.Queries()
		if err != nil {
			log.WithFields(
				log.Fields{
					"manifest_id": item.ManifestId,
					"upload_id":   item.UploadId,
				}).Error("error getting pg queries for bucket update: ", err)
			continue
		}
		switch err := pg.UpdateBucketForFile(context.Background(), item.UploadId, stOrgItem.storageBucket, targetPath, stOrgItem.organizationId); err.(type) {
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

	region, exists := pkg.GetRegion(stOrgItem.storageBucket)
	if !exists {
		return errors.New(fmt.Sprintf("Could not determine region from bucket name %s", stOrgItem.storageBucket))
	}
	options := func(o *s3.Options) {
		o.Region = region.RegionCode
	}

	_, err := s.s3.CopyObject(context.Background(), &params, options)
	if err != nil {
		return err
	}

	return nil
}

func (s *UploadMoveStore) Close() {
	if err := s.pgManager.Close(); err != nil {
		log.Warn("error closing pgManager: ", err)
	}
}
