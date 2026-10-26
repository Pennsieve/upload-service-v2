package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	dyQueriesNs "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/upload"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/bucketregion"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/runresults"
	"github.com/pennsieve/pennsieve-upload-service-v2/service/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// Importing a workflow run's results into a dataset (a "Pennsieve Dataset"
// data target; see pkg/runresults). The compute node's status-updater calls
// POST /manifest/run-results/import with the run's callback token, in pages
// of up to maxRunImportObjects staged files, then polls
// GET /manifest/run-results/import until every file is in the dataset or
// one failed.

// maxRunImportObjects bounds one request: registering and queueing this
// many files stays well inside API Gateway's 29s.
const maxRunImportObjects = 1000

// runImportNamespace makes a run target's manifest id (and its files' upload
// ids) the same on every request, so retries and further pages of the same
// import land on the same manifest and rows instead of making new ones.
var runImportNamespace = uuid.MustParse("4b8f3c2e-6a1d-4f7e-9c35-2d1e8a7b6f90")

var runTargetIdRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type runResultsObject struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

type runResultsImportRequest struct {
	RunId        string             `json:"runId"`
	TargetId     string             `json:"targetId"`
	TargetFolder string             `json:"targetFolder,omitempty"`
	OnConflict   string             `json:"onConflict,omitempty"`
	Objects      []runResultsObject `json:"objects"`
}

type runResultsImportResponse struct {
	ManifestNodeId string `json:"manifestNodeId"`
	Queued         int    `json:"queued"`
}

// runImportManifestId is the manifest a run target imports into dataset.
func runImportManifestId(runId, targetId, datasetNodeId string) string {
	return uuid.NewSHA1(runImportNamespace, []byte(runId+"/"+targetId+"/"+datasetNodeId)).String()
}

// runImportUploadId is a staged file's upload id in its manifest.
func runImportUploadId(manifestId, key string) string {
	return uuid.NewSHA1(uuid.MustParse(manifestId), []byte(key)).String()
}

// planRunImport turns staged files into the manifest's file rows and the
// copy jobs that fill them. src and dest name the run's bucket and the
// manifest's storage; the files must be below the target's prefix.
func planRunImport(req runResultsImportRequest, manifestId string, src runresults.Results, dest storage.Resolution, destRegion string) ([]manifestFile.FileDTO, []runresults.Job, error) {
	prefix := runresults.Prefix(req.RunId, req.TargetId)
	keyPrefix := dest.KeyPrefix(manifestId)
	files := make([]manifestFile.FileDTO, 0, len(req.Objects))
	jobs := make([]runresults.Job, 0, len(req.Objects))
	seen := make(map[string]bool, len(req.Objects))
	for i, o := range req.Objects {
		if o.Size < 0 {
			return nil, nil, fmt.Errorf("objects[%d].size must be non-negative", i)
		}
		if seen[o.Key] {
			return nil, nil, fmt.Errorf("objects[%d]: %q is listed twice", i, o.Key)
		}
		seen[o.Key] = true
		folder, name, err := runresults.FilePlacement(prefix, o.Key, req.TargetFolder)
		if err != nil {
			return nil, nil, fmt.Errorf("objects[%d]: %w", i, err)
		}
		uploadId := runImportUploadId(manifestId, o.Key)
		destKey := keyPrefix + "/" + uploadId
		files = append(files, manifestFile.FileDTO{
			UploadID:   uploadId,
			S3Key:      destKey,
			TargetPath: folder,
			TargetName: name,
		})
		jobs = append(jobs, runresults.Job{
			ManifestId:   manifestId,
			UploadId:     uploadId,
			RunId:        req.RunId,
			OnConflict:   req.OnConflict,
			OrgIntId:     dest.OrganizationId,
			DatasetIntId: dest.DatasetId,
			Size:         o.Size,
			Source: runresults.Source{
				Bucket:     src.Bucket,
				Key:        o.Key,
				ReadPrefix: prefix,
				RoleArn:    src.RoleArn,
				ExternalId: src.ExternalId,
				Region:     src.Region,
			},
			Dest: runresults.Dest{
				Bucket:    dest.StorageBucket,
				Key:       destKey,
				KeyPrefix: keyPrefix,
				Region:    destRegion,
			},
		})
	}
	return files, jobs, nil
}

// checkRunImportRequest validates a request against the callback token's
// run (from the Authorization header).
func checkRunImportRequest(req runResultsImportRequest, authorization string) (token string, err error) {
	service, runId, token, ok := runresults.Token(authorization)
	if !ok || service != "workflow-service" {
		return "", errors.New("run imports take the run's callback token")
	}
	if req.RunId == "" || req.RunId != runId {
		return "", errors.New("runId must be the callback token's run")
	}
	if !runTargetIdRE.MatchString(req.TargetId) {
		return "", errors.New("targetId must be the data target's node id")
	}
	if len(req.Objects) == 0 || len(req.Objects) > maxRunImportObjects {
		return "", fmt.Errorf("objects must list 1 to %d files", maxRunImportObjects)
	}
	if !validOnConflict(req.OnConflict) {
		return "", errors.New("onConflict must be one of: keepBoth, replace")
	}
	return token, nil
}

// postRunResultsImportRoute registers a page of a run target's staged files
// on its manifest (made on the first page) and queues their copies.
func postRunResultsImportRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	var req runResultsImportRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		return errResp(400, "invalid request body")
	}
	token, err := checkRunImportRequest(req, request.Headers["authorization"])
	if err != nil {
		return errResp(400, err.Error())
	}
	if req.OnConflict == "" {
		req.OnConflict = onConflictKeepBoth
	}
	queueURL := os.Getenv("RUN_RESULTS_IMPORT_QUEUE_URL")
	defaultStorageBucket := os.Getenv("DEFAULT_STORAGE_BUCKET")
	if queueURL == "" || defaultStorageBucket == "" {
		log.Error("run imports: RUN_RESULTS_IMPORT_QUEUE_URL or DEFAULT_STORAGE_BUCKET not configured")
		return errResp(500, "Run imports not configured")
	}
	ctx := context.Background()
	logger := log.WithFields(log.Fields{"runId": req.RunId, "targetId": req.TargetId, "datasetId": claims.DatasetClaim.NodeId})

	// The token's run and its compute node: where to read. The dataset
	// permission was checked by the authorizer, for the run's creator.
	validator := &runresults.Validator{Lambda: store.lambdaClient, FunctionName: os.Getenv("CALLBACK_VALIDATOR_FUNCTION")}
	run, err := validator.Validate(ctx, req.RunId, token)
	if errors.Is(err, runresults.ErrUnauthorized) {
		return errResp(403, "callback token not valid for this run")
	}
	if err != nil {
		logger.WithError(err).Error("run imports: checking the callback token")
		return errResp(500, "Could not check the run")
	}
	locator := &runresults.Locator{
		DB:            store.dynamodb,
		NodesTable:    os.Getenv("COMPUTE_NODES_TABLE"),
		AccountsTable: os.Getenv("ACCOUNTS_TABLE"),
		Region:        os.Getenv("REGION"),
	}
	src, err := locator.Locate(ctx, run)
	if err != nil {
		logger.WithError(err).Error("run imports: locating the run's results")
		return errResp(500, "Could not find the run's results")
	}

	manifestId := runImportManifestId(req.RunId, req.TargetId, claims.DatasetClaim.NodeId)
	if _, err := store.dy.GetManifestById(ctx, store.tableName, manifestId); err != nil {
		if err := store.dy.CreateManifest(ctx, store.tableName, dydb.ManifestTable{
			ManifestId:     manifestId,
			DatasetId:      claims.DatasetClaim.IntId,
			DatasetNodeId:  claims.DatasetClaim.NodeId,
			OrganizationId: claims.OrgClaim.IntId,
			UserId:         claims.UserClaim.Id,
			Status:         manifest.Initiated.String(),
			DateCreated:    time.Now().Unix(),
		}); err != nil {
			logger.WithError(err).Error("run imports: creating the manifest")
			return errResp(500, "Could not create the manifest")
		}
	}

	pgdb, err := pgQueries.ConnectRDS()
	if err != nil {
		logger.WithError(err).Error("run imports: connecting to RDS")
		return errResp(500, "Internal error")
	}
	defer pgdb.Close()
	dest, err := storage.ResolveForManifest(ctx, manifestId, store.tableName, defaultStorageBucket, dyQueriesNs.New(store.dynamodb), pgQueries.New(pgdb))
	if err != nil {
		logger.WithError(err).Error("run imports: resolving the storage bucket")
		return errResp(500, "Failed to resolve storage bucket")
	}
	destRegion, err := bucketregion.Resolve(ctx, store.s3Client, dest.StorageBucket)
	if err != nil {
		logger.WithError(err).Error("run imports: storage bucket region")
		return errResp(500, "Failed to determine storage bucket region")
	}

	files, jobs, err := planRunImport(req, manifestId, src, *dest, destRegion)
	if err != nil {
		return errResp(400, err.Error())
	}
	upload.PackageTypeResolver(files)
	if _, err := store.dy.SyncFiles(manifestId, files, nil, store.tableName, store.fileTableName); err != nil {
		logger.WithError(err).Error("run imports: registering files")
		return errResp(500, "Could not register the files")
	}
	if failed := enqueueRunImportJobs(ctx, store.sqsClient, queueURL, jobs); failed > 0 {
		logger.Errorf("run imports: %d of %d copies not queued", failed, len(jobs))
		// The caller retries the page: registering is idempotent, and the
		// copy lambda skips files already in the dataset.
		return errResp(503, "Could not queue every copy; retry")
	}
	logger.WithField("manifestId", manifestId).Infof("run imports: %d files queued", len(jobs))
	body, _ := json.Marshal(runResultsImportResponse{ManifestNodeId: manifestId, Queued: len(jobs)})
	return &events.APIGatewayV2HTTPResponse{StatusCode: 200, Body: string(body)}, nil
}

// enqueueRunImportJobs queues one message per copy, ten per batch; it
// returns how many weren't queued.
func enqueueRunImportJobs(ctx context.Context, client *sqs.Client, queueURL string, jobs []runresults.Job) int {
	failed := 0
	for start := 0; start < len(jobs); start += 10 {
		end := min(start+10, len(jobs))
		entries := make([]sqsTypes.SendMessageBatchRequestEntry, 0, end-start)
		for i, j := range jobs[start:end] {
			body, _ := json.Marshal(j)
			entries = append(entries, sqsTypes.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprintf("j%d", i)),
				MessageBody: aws.String(string(body)),
			})
		}
		out, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(queueURL), Entries: entries})
		if err != nil {
			log.WithError(err).Warn("run imports: SendMessageBatch failed")
			failed += len(entries)
			continue
		}
		failed += len(out.Failed)
	}
	return failed
}

// runImportStatus is how far a run target's import is: its files by state,
// and the first failures with their reasons.
type runImportStatus struct {
	ManifestNodeId string             `json:"manifestNodeId"`
	Total          int                `json:"total"`
	Imported       int                `json:"imported"`
	Failed         int                `json:"failed"`
	InProgress     int                `json:"inProgress"`
	Failures       []runImportFailure `json:"failures,omitempty"`
}

type runImportFailure struct {
	Path    string `json:"path"`
	Message string `json:"message,omitempty"`
}

// maxReportedFailures bounds the failures a status lists.
const maxReportedFailures = 10

type runImportFileRow struct {
	Status   string `dynamodbav:"Status"`
	FilePath string `dynamodbav:"FilePath"`
	FileName string `dynamodbav:"FileName"`
	Message  string `dynamodbav:"Message"`
}

// countRunImport tallies a manifest's file rows: Finalized and Verified are
// in the dataset; Failed and FailedOrphan failed; the rest still moving.
func countRunImport(manifestId string, rows []runImportFileRow) runImportStatus {
	s := runImportStatus{ManifestNodeId: manifestId, Total: len(rows)}
	for _, r := range rows {
		switch r.Status {
		case manifestFile.Finalized.String(), manifestFile.Verified.String():
			s.Imported++
		case manifestFile.Failed.String(), manifestFile.FailedOrphan.String():
			s.Failed++
			if len(s.Failures) < maxReportedFailures {
				p := r.FileName
				if r.FilePath != "" {
					p = r.FilePath + "/" + r.FileName
				}
				s.Failures = append(s.Failures, runImportFailure{Path: p, Message: r.Message})
			}
		default:
			s.InProgress++
		}
	}
	return s
}

// getRunResultsImportRoute reports a run target's import (manifest_id).
func getRunResultsImportRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	manifestId := request.QueryStringParameters["manifest_id"]
	if !isValidUUID(manifestId) {
		return errResp(400, "manifest_id must be a UUID")
	}
	ctx := context.Background()
	m, err := store.dy.GetManifestById(ctx, store.tableName, manifestId)
	if err != nil {
		return errResp(404, "Manifest not found")
	}
	if m.DatasetNodeId != claims.DatasetClaim.NodeId {
		return errResp(403, "Manifest does not belong to this dataset")
	}
	var rows []runImportFileRow
	var start map[string]dyTypes.AttributeValue
	for {
		out, err := store.dynamodb.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(store.fileTableName),
			KeyConditionExpression: aws.String("ManifestId = :m"),
			ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
				":m": &dyTypes.AttributeValueMemberS{Value: manifestId},
			},
			ProjectionExpression:     aws.String("#s, FilePath, FileName, #msg"),
			ExpressionAttributeNames: map[string]string{"#s": "Status", "#msg": "Message"},
			ExclusiveStartKey:        start,
		})
		if err != nil {
			log.WithError(err).WithField("manifestId", manifestId).Error("run imports: reading file rows")
			return errResp(500, "Could not read the import's files")
		}
		for _, item := range out.Items {
			var r runImportFileRow
			if err := attributevalue.UnmarshalMap(item, &r); err == nil {
				rows = append(rows, r)
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		start = out.LastEvaluatedKey
	}
	body, _ := json.Marshal(countRunImport(manifestId, rows))
	return &events.APIGatewayV2HTTPResponse{StatusCode: 200, Body: string(body)}, nil
}
