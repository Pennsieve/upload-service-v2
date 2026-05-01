package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdaTypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	log "github.com/sirupsen/logrus"
)

type sweeper struct {
	dy     *dynamodb.Client
	lambda *lambda.Client

	manifestTable    string
	archiveLambdaArn string

	maxAgeDays       int
	maxInvokesPerRun int
	dryRun           bool
}

// archiveEvent mirrors the payload the archive_lambda expects (defined in
// lambda/archiver/handler/handler.go). Keep the field names / JSON tags in
// sync with that structure.
type archiveEvent struct {
	ManifestId     string `json:"manifest_id"`
	OrganizationId int64  `json:"organization_id"`
	DatasetId      int64  `json:"dataset_id"`
	RemoveFromDB   bool   `json:"remove_from_db"`
}

// nonArchivedStatuses enumerates every manifest.Status value except
// Archived. The sweeper Queries each one against ManifestStatusIndex to
// build the eligible-for-archival set. New non-Archived statuses MUST be
// added here or they will silently never get archived.
var nonArchivedStatuses = []manifest.Status{
	manifest.Initiated,
	manifest.Uploading,
	manifest.Completed,
	manifest.Cancelled,
}

// run queries ManifestStatusIndex once per non-Archived status, picking up
// rows older than MaxAgeDays, and invokes archive_lambda async for each
// eligible manifest up to maxInvokesPerRun.
//
// Replaces the previous Scan-with-FilterExpression approach. With the GSI
// on (Status, DateCreated), each Query reads only the rows that match — no
// per-row filter overhead, and no read charge for archived items.
func (s *sweeper) run(ctx context.Context) (Result, error) {
	res := Result{
		DryRun:     s.dryRun,
		MaxAgeDays: s.maxAgeDays,
	}

	cutoff := time.Now().Add(-time.Duration(s.maxAgeDays) * 24 * time.Hour).Unix()

	for _, status := range nonArchivedStatuses {
		done, err := s.runForStatus(ctx, status, cutoff, &res)
		if err != nil {
			return res, err
		}
		if done {
			return res, nil
		}
	}

	return res, nil
}

// runForStatus pages through ManifestStatusIndex for a single status,
// invoking archive_lambda for each eligible row. Returns done=true if the
// caller should stop (MaxInvokesPerRun reached); err is set only on
// unrecoverable Query failures.
func (s *sweeper) runForStatus(ctx context.Context, status manifest.Status, cutoff int64, res *Result) (bool, error) {
	p := dynamodb.NewQueryPaginator(s.dy, &dynamodb.QueryInput{
		TableName:              aws.String(s.manifestTable),
		IndexName:              aws.String("ManifestStatusIndex"),
		KeyConditionExpression: aws.String("#s = :status AND DateCreated < :cutoff"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":status": &dyTypes.AttributeValueMemberS{Value: status.String()},
			":cutoff": &dyTypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", cutoff)},
		},
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("query %s: %v", status.String(), err))
			return false, err
		}
		res.ManifestsScanned += int(page.ScannedCount)

		for _, item := range page.Items {
			var m dydb.ManifestTable
			if err := attributevalue.UnmarshalMap(item, &m); err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("unmarshal manifest: %v", err))
				continue
			}
			// Status isn't projected (it's the index hash key, but
			// UnmarshalMap won't see it from key-only context); restore it
			// so downstream logging is accurate.
			m.Status = status.String()
			res.ManifestsEligible++

			if res.InvokesAttempted >= s.maxInvokesPerRun {
				log.WithFields(log.Fields{
					"eligible":    res.ManifestsEligible,
					"max_per_run": s.maxInvokesPerRun,
					"manifest_id": m.ManifestId,
				}).Info("hit MaxInvokesPerRun; deferring remaining manifests to next run")
				return true, nil
			}

			logger := log.WithFields(log.Fields{
				"manifest_id":     m.ManifestId,
				"organization_id": m.OrganizationId,
				"dataset_id":      m.DatasetId,
				"status":          m.Status,
				"date_created":    m.DateCreated,
				"age_days":        int(time.Since(time.Unix(m.DateCreated, 0)).Hours() / 24),
			})

			if s.dryRun {
				logger.Info("dry-run: would invoke archive_lambda")
				res.InvokesAttempted++
				continue
			}

			if err := s.invokeArchive(ctx, m); err != nil {
				res.InvokesFailed++
				res.Errors = append(res.Errors, fmt.Sprintf("invoke archive for %s: %v", m.ManifestId, err))
				logger.WithError(err).Warn("archive invoke failed")
				continue
			}
			res.InvokesAttempted++
			logger.Info("archive_lambda invoked")
		}
	}

	return false, nil
}

func (s *sweeper) invokeArchive(ctx context.Context, m dydb.ManifestTable) error {
	ev := archiveEvent{
		ManifestId:     m.ManifestId,
		OrganizationId: m.OrganizationId,
		DatasetId:      m.DatasetId,
		RemoveFromDB:   true,
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal archive event: %w", err)
	}
	_, err = s.lambda.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(s.archiveLambdaArn),
		InvocationType: lambdaTypes.InvocationTypeEvent, // async: we don't wait
		Payload:        payload,
	})
	return err
}

func timestampMillis() int64 { return time.Now().UnixMilli() }
