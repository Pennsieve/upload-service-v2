package handler

import (
	"context"
	"encoding/json"
	"os"

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
)

var (
	ChangelogClient       *changelog.Client
	SNSClient             *sns.Client
	SNSTopic              string
	FileFinalizedTopic    string
	S3Client              *s3.Client
	DynamoClient          *dynamodb.Client
	SQSClient             *sqs.Client
	ManifestTableName     string
	ManifestFileTableName string
	JobSQSQueueId         string
	DeleteSQSQueueId      string
	PusherConfig          *ps.Config
	PusherClient          *pusher.Client
)

// init runs on cold start of lambda and configures logging and looks up env vars.
func init() {

	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	ManifestFileTableName = os.Getenv("MANIFEST_FILE_TABLE")
	ManifestTableName = os.Getenv("MANIFEST_TABLE")
	JobSQSQueueId = os.Getenv("JOBS_QUEUE_ID")
	DeleteSQSQueueId = os.Getenv("DELETE_QUEUE_ID")
	SNSTopic = os.Getenv("IMPORTED_SNS_TOPIC")
	FileFinalizedTopic = os.Getenv("FILE_FINALIZED_TOPIC")
}

// InitializeClients initializes the clients required by the Handler. Depends on values
// that are initialized by the package init() function.
// Separated out from init() since in its current state it really shouldn't be called when tests run
func InitializeClients() {
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

	SNSClient = sns.NewFromConfig(cfg)
	S3Client = s3.NewFromConfig(cfg)
	SNSTopic = os.Getenv("IMPORTED_SNS_TOPIC")
	FileFinalizedTopic = os.Getenv("FILE_FINALIZED_TOPIC")
	DynamoClient = dynamodb.NewFromConfig(cfg)
	SQSClient = sqs.NewFromConfig(cfg)
	ChangelogClient = changelog.NewClient(*SQSClient, JobSQSQueueId)
}

// filterLiveRecords splits a batch into messages that carry an S3 event and
// those that do not. Heartbeats are emitted once a minute by the
// upload_lambda_heartbeat EventBridge rule (modules/upload-service-v2/
// cloudwatch.tf in clin-infrastructure) purely to keep the SQS pollers and
// the execution environment warm; they carry no S3 Records. Anything else
// unparseable is dropped by the same guard so the Records[0] accesses
// downstream stay safe.
func filterLiveRecords(records []events.SQSMessage) ([]events.SQSMessage, int) {
	live := records[:0]
	dropped := 0
	for _, m := range records {
		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(m.Body), &parsedS3Event); err != nil || len(parsedS3Event.Records) == 0 {
			dropped++
			continue
		}
		live = append(live, m)
	}
	return live, dropped
}

// Handler implements the function that is called when new SQS Events arrive.
func Handler(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {

	eventResponse := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}

	// Discard heartbeats before touching Postgres. A heartbeat needs no
	// database, and connecting anyway had two costs: ~43k RDS connections a
	// month for warmth pings, and — worse — during an RDS outage every ping
	// failed and landed in the DLQ, so the DLQ alarm paged on a liveness
	// signal instead of on real upload failures.
	liveRecords, heartbeatCount := filterLiveRecords(sqsEvent.Records)
	if heartbeatCount > 0 {
		log.Debugf("Dropped %d heartbeat/non-S3 message(s) from batch", heartbeatCount)
	}
	if len(liveRecords) == 0 {
		return eventResponse, nil
	}
	sqsEvent.Records = liveRecords

	// db.Close() must be deferred only after the error check: ConnectRDS
	// returns a nil *sql.DB on failure, and Close() on that panics with a
	// nil-pointer dereference, replacing the real error with a stack trace.
	db, err := pgQueries.ConnectRDS()
	if err != nil {
		return eventResponse, err
	}
	defer db.Close()

	// Define store without Postgres connection (as this is different depending on the manifest/org)
	s := NewUploadHandlerStore(
		db,
		DynamoClient,
		SNSClient,
		S3Client,
		ManifestFileTableName,
		ManifestTableName,
		SNSTopic,
		FileFinalizedTopic,
		PusherClient,
		ChangelogClient,
		SQSClient,
		DeleteSQSQueueId)

	eventResponse, err = s.Handler(ctx, sqsEvent)
	if err != nil {
		return eventResponse, err
	}
	return eventResponse, nil
}
