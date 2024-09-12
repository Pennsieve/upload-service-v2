package handler

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	ps "github.com/pennsieve/pennsieve-go-core/pkg/models/pusher"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pusher/pusher-http-go/v5"
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
	PusherConfig          *ps.Config
	PusherClient          *pusher.Client
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

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	ssmsvc := ssm.NewFromConfig(cfg)
	param, err := ssmsvc.GetParameter(context.Background(), &ssm.GetParameterInput{
		Name:           aws.String("/ops/pusher-config"),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		log.Warnf("LoadDefaultConfig: %v\n", err)
	} else {
		value := *param.Parameter.Value
		err = json.Unmarshal([]byte(value), &PusherConfig)
		if err != nil {
			log.Fatalf("ConvertPusherCongifToStruct: %v\n", err)
		}

		PusherClient = &pusher.Client{
			AppID:   PusherConfig.AppId,
			Key:     PusherConfig.Key,
			Secret:  PusherConfig.Secret,
			Cluster: PusherConfig.Cluster,
			Secure:  true,
		}
	}

	ManifestFileTableName = os.Getenv("MANIFEST_FILE_TABLE")
	ManifestTableName = os.Getenv("MANIFEST_TABLE")
	jobSQSQueueId := os.Getenv("JOBS_QUEUE_ID")
	SNSClient = sns.NewFromConfig(cfg)
	S3Client = s3.NewFromConfig(cfg)
	SNSTopic = os.Getenv("IMPORTED_SNS_TOPIC")
	DynamoClient = dynamodb.NewFromConfig(cfg)
	ChangelogClient = changelog.NewClient(*sqs.NewFromConfig(cfg), jobSQSQueueId)
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
	s := NewUploadHandlerStore(
		db,
		DynamoClient,
		SNSClient,
		S3Client,
		ManifestFileTableName,
		ManifestTableName,
		SNSTopic,
		PusherClient,
		ChangelogClient)

	eventResponse, err = s.Handler(ctx, sqsEvent)
	if err != nil {
		return eventResponse, err
	}
	return eventResponse, nil
}
