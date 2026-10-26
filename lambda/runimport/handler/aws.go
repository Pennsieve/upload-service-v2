package handler

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/runresults"
	log "github.com/sirupsen/logrus"
)

// sessionDuration is how long a role session lasts; a copy takes at most
// the lambda's 15 minutes, and credentials refresh before they expire.
const sessionDuration = time.Hour

// sessions keeps an S3 client per role session (role, external id and
// session policy), across the lambda's invocations.
type sessions struct {
	cfg  aws.Config
	sts  *sts.Client
	name string

	mu      sync.Mutex
	clients map[string]*s3.Client
}

func (s *sessions) client(region, roleArn, externalId, policy string) *s3.Client {
	key := region + "\x00" + roleArn + "\x00" + externalId + "\x00" + policy
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[key]; ok {
		return c
	}
	creds := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(s.sts, roleArn, func(o *stscreds.AssumeRoleOptions) {
		o.RoleSessionName = s.name
		o.Duration = sessionDuration
		o.Policy = aws.String(policy)
		if externalId != "" {
			o.ExternalID = aws.String(externalId)
		}
	}), func(o *aws.CredentialsCacheOptions) {
		o.ExpiryWindow = 5 * time.Minute
	})
	c := s3.NewFromConfig(s.cfg, func(o *s3.Options) {
		o.Region = region
		o.Credentials = creds
	})
	if s.clients == nil {
		s.clients = map[string]*s3.Client{}
	}
	s.clients[key] = c
	return c
}

// s3Writer stores files with sessions of the storage credentials role,
// limited to the manifest's prefix, as uploading clients are. Multipart,
// with a SHA-256 checksum the upload lambda records.
type s3Writer struct {
	sessions *sessions
	roleArn  string
}

func (w s3Writer) Put(ctx context.Context, dest runresults.Dest, body io.Reader, size int64, tagging string) error {
	client := w.sessions.client(dest.Region, w.roleArn, "", runresults.WriteSessionPolicy(dest.Bucket, dest.KeyPrefix))
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partSize(size)
		u.Concurrency = 4
	})
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(dest.Bucket),
		Key:               aws.String(dest.Key),
		Body:              body,
		ChecksumAlgorithm: s3Types.ChecksumAlgorithmSha256,
		Tagging:           aws.String(tagging),
	})
	return err
}

// partSize keeps a file within S3's 10,000 parts, from 16 MiB up.
func partSize(size int64) int64 {
	const minPart = 16 << 20
	p := size/9000 + 1
	if p < minPart {
		return minPart
	}
	return p
}

var copier *Copier

// InitializeClients builds the copier from the environment, on cold start.
func InitializeClients() {
	log.SetFormatter(&log.JSONFormatter{})
	if ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(ll)
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v", err)
	}
	s := &sessions{cfg: cfg, sts: sts.NewFromConfig(cfg), name: "upload-service-run-import"}
	copier = &Copier{
		Reader:                s3Reader{sessions: s},
		Writer:                s3Writer{sessions: s, roleArn: os.Getenv("STORAGE_CREDENTIALS_ROLE_ARN")},
		Files:                 dynamodb.NewFromConfig(cfg),
		FileTableName:         os.Getenv("MANIFEST_FILE_TABLE"),
		Queue:                 sqs.NewFromConfig(cfg),
		UploadTriggerQueueURL: os.Getenv("UPLOAD_TRIGGER_QUEUE_URL"),
	}
}

// Handle is the lambda's entry point (an SQS event source).
func Handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	return copier.Handle(ctx, ev)
}
