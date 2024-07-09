package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

var uploadBucket string
var defaultStorageBucket string
var FileTableName string
var TableName string

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

	FileTableName = os.Getenv("FILES_TABLE")
	TableName = os.Getenv("MANIFEST_TABLE")

	// Get Postgres connection
	db, err := pgQueries.ConnectRDS()
	//Session.pgClient = db
	if err != nil {
		log.Fatalf("Cannot connect to the Pennsieve Postgres Proxy.")
	}

	store := NewUploadMoveStore(db, dynamodb.NewFromConfig(cfg), s3.NewFromConfig(cfg))
	//goland:noinspection GoUnhandledErrorResult
	defer store.db.Close()

	// start database keepalive
	ticker := time.NewTicker(1 * time.Minute)
	go store.KeepAlive(context.Background(), ticker)

	uploadBucket = os.Getenv("UPLOAD_BUCKET")
	defaultStorageBucket = os.Getenv("STORAGE_BUCKET")

	walker := make(fileWalk)

	// Walk over all files in IMPORTED status and make available on channel for processors.
	go func() {
		defer func() {
			close(walker)
		}()

		// Get all the files in Uploaded State from Dynamodb and put on channel.
		if err := store.manifestFileWalk(walker); err != nil {
			log.Fatalf("Manifest File Walker failed: %v", err)
		}
	}()

	timeout := FileMoveTimeout()

	// Initiate the upload workers
	for w := 1; w <= nrWorkers; w++ {
		processWg.Add(1)
		log.Debug("starting worker:", w)
		go store.moveFile(w, timeout, walker)
	}

	// Wait until all processors are completed.
	processWg.Wait()

	// stop database keepalive
	ticker.Stop()

	log.Println("Finished with task.")
}
