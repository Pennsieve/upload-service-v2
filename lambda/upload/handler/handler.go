package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"os"
)

var manifestSession ManifestSession
var ManifestFileTableName, ManifestTableName string

// init runs on cold start of lambda and gets jwt key-sets from Cognito user pools.
func init() {

	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	ManifestFileTableName = os.Getenv("FILES_TABLE")
	ManifestTableName = os.Getenv("MANIFEST_TABLE")

	manifestSession = ManifestSession{
		FileTableName: os.Getenv("MANIFEST_FILE_TABLE"),
		TableName:     os.Getenv("MANIFEST_TABLE"),
		Client:        dynamodb.NewFromConfig(cfg),
		SNSClient:     sns.NewFromConfig(cfg),
		SNSTopic:      os.Getenv("IMPORTED_SNS_TOPIC"),
		S3Client:      s3.NewFromConfig(cfg),
	}
}

// Handler implements the function that is called when new SQS Events arrive.
func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {

	db, err := pgQueries.ConnectRDSWithOrg(2)
	if err != nil {
		return err
	}

	// Define store without Postgres connection (as this is different depending on the manifest/org)
	s := NewUploadHandlerStore(db, manifestSession.Client, manifestSession.SNSClient,
		manifestSession.S3Client, manifestSession.FileTableName, manifestSession.TableName)

	err = s.Handler(ctx, sqsEvent)
	return err
}
