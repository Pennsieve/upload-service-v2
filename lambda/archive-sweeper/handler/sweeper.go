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

// run scans manifest_table with a FilterExpression bounding by age and
// Status, then invokes archive_lambda async for each eligible manifest up
// to maxInvokesPerRun.
//
// Uses Scan (not Query) because the manifest table has no index on
// DateCreated/Status. The table is small (one row per manifest) relative to
// manifest_files, so a full scan is acceptable for a daily job.
func (s *sweeper) run(ctx context.Context) (Result, error) {
	res := Result{
		DryRun:     s.dryRun,
		MaxAgeDays: s.maxAgeDays,
	}

	cutoff := time.Now().Add(-time.Duration(s.maxAgeDays) * 24 * time.Hour).Unix()

	p := dynamodb.NewScanPaginator(s.dy, &dynamodb.ScanInput{
		TableName: aws.String(s.manifestTable),
		FilterExpression: aws.String(
			"DateCreated < :cutoff AND #s <> :archived",
		),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":cutoff":   &dyTypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", cutoff)},
			":archived": &dyTypes.AttributeValueMemberS{Value: manifest.Archived.String()},
		},
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("scan: %v", err))
			return res, err
		}
		res.ManifestsScanned += int(page.ScannedCount)

		for _, item := range page.Items {
			var m dydb.ManifestTable
			if err := attributevalue.UnmarshalMap(item, &m); err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("unmarshal manifest: %v", err))
				continue
			}
			res.ManifestsEligible++

			if res.InvokesAttempted >= s.maxInvokesPerRun {
				// Leave the rest for the next scheduled run; bail out of the
				// page (also stops paginator on next HasMorePages check).
				log.WithFields(log.Fields{
					"eligible":     res.ManifestsEligible,
					"max_per_run":  s.maxInvokesPerRun,
					"manifest_id":  m.ManifestId,
				}).Info("hit MaxInvokesPerRun; deferring remaining manifests to next run")
				return res, nil
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

	return res, nil
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
