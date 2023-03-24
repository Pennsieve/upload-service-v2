package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	log "github.com/sirupsen/logrus"
	"os"
)

var store *ArchiverStore
var archiverBucket string

// init runs on cold start of lambda and gets jwt key-sets from Cognito user pools.
func init() {

	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	manifestFileTableName := os.Getenv("MANIFEST_FILE_TABLE")
	manifestTableName := os.Getenv("MANIFEST_TABLE")
	archiverBucket = os.Getenv("ARCHIVER_BUCKET")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	store = NewArchiverStore(client, s3Client, manifestFileTableName, manifestTableName)

}

type ArchiveEvent struct {
	ManifestId     string `json:"manifest_id"`
	OrganizationId int64  `json:"organization_id"`
	DatasetId      int64  `json:"dataset_id"`
}

func ManifestHandler(event ArchiveEvent) error {

	log.WithFields(
		log.Fields{
			"manifest_id":     event.ManifestId,
			"organization_id": event.OrganizationId,
			"dataset_id":      event.DatasetId,
		}).Info("Manifest Archiver called.")

	ctx := context.Background()
	csvFileName := fmt.Sprintf("manifest_archive_%s", event.ManifestId)
	_, err := store.writeCSVFile(ctx, csvFileName, event.ManifestId)

	_, err = store.writeManifestToS3(ctx, csvFileName, event.OrganizationId, event.DatasetId)

	if err != nil {
		return err
	}

	err = store.UpdateManifestStatus(ctx, store.tableName, event.ManifestId, manifest.Archived)
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id":     event.ManifestId,
				"organization_id": event.OrganizationId,
				"dataset_id":      event.DatasetId,
			}).Error("Cannot update manifest to 'Archived'.")
		return err
	}

	err = store.removeManifestFiles(ctx, event.ManifestId)
	if err != nil {
		return err
	}

	return nil
}
