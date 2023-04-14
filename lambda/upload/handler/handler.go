package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"os"
)

var (
	ChangelogClient       *changelog.Client
	SNSClient             *sns.Client
	SNSTopic              string
	S3Client              *s3.Client
	DynamoClient          *dynamodb.Client
	ManifestTableName     string
	ManifestFileTableName string
)

// init runs on cold start of lambda and gets jwt key-sets from Cognito user pools.
func init() {

	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	jobSQSQueueId := os.Getenv("JOBS_QUEUE_ID")

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	ManifestFileTableName = os.Getenv("MANIFEST_FILE_TABLE")
	ManifestTableName = os.Getenv("MANIFEST_TABLE")
	SNSClient = sns.NewFromConfig(cfg)
	S3Client = s3.NewFromConfig(cfg)
	SNSTopic = os.Getenv("IMPORTED_SNS_TOPIC")
	DynamoClient = dynamodb.NewFromConfig(cfg)
	ChangelogClient = changelog.NewChangeLogClient(*sqs.NewFromConfig(cfg), jobSQSQueueId)
}

// Handler implements the function that is called when new SQS Events arrive.
func Handler(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {

	eventResponse := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}

	db, err := pgQueries.ConnectRDS()
	defer db.Close()
	if err != nil {
		return eventResponse, err
	}

	// Define store without Postgres connection (as this is different depending on the manifest/org)
	s := NewUploadHandlerStore(db, DynamoClient, SNSClient, S3Client, ManifestFileTableName, ManifestTableName, SNSTopic)

	eventResponse, err = s.Handler(ctx, sqsEvent)
	if err != nil {
		return eventResponse, err
	}
	return eventResponse, nil
}
