package handler

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	log "github.com/sirupsen/logrus"
)

// resetFailedOrphans finds files among `incoming` whose current DynamoDB
// status is FailedOrphan and flips them back to Registered. This is the
// server-side half of the retry-on-reupload flow: the reconciler-marked
// orphan becomes eligible for a fresh upload attempt when the agent
// re-syncs the manifest.
//
// Uses BatchGetItem to identify the subset (cheap reads), then conditional
// UpdateItem per match (small writes — typical case is a handful of files
// per manifest, not thousands). If no file is currently FailedOrphan, this
// is a couple of BatchGetItem calls and returns nil immediately.
//
// Errors are non-fatal for the caller: the subsequent SyncFiles call will
// still run and produce a correct response for every non-FailedOrphan file.
// Only the FailedOrphan subset would fail to retry on this request, and the
// operator can retry.
func resetFailedOrphans(
	ctx context.Context,
	dy *dynamodb.Client,
	fileTable string,
	manifestID string,
	incoming []manifestFile.FileDTO,
) error {
	if len(incoming) == 0 {
		return nil
	}

	// BatchGetItem caps at 100 items per call.
	const batchGetLimit = 100
	var orphanUploadIDs []string
	orphanStatus := manifestFile.FailedOrphan.String()

	for start := 0; start < len(incoming); start += batchGetLimit {
		end := start + batchGetLimit
		if end > len(incoming) {
			end = len(incoming)
		}
		keys := make([]map[string]dyTypes.AttributeValue, 0, end-start)
		for _, f := range incoming[start:end] {
			if f.UploadID == "" {
				continue
			}
			keys = append(keys, map[string]dyTypes.AttributeValue{
				"ManifestId": &dyTypes.AttributeValueMemberS{Value: manifestID},
				"UploadId":   &dyTypes.AttributeValueMemberS{Value: f.UploadID},
			})
		}
		if len(keys) == 0 {
			continue
		}
		out, err := dy.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]dyTypes.KeysAndAttributes{
				fileTable: {
					Keys:                 keys,
					ProjectionExpression: aws.String("UploadId, #s"),
					ExpressionAttributeNames: map[string]string{
						"#s": "Status",
					},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("batchget: %w", err)
		}
		for _, item := range out.Responses[fileTable] {
			var uid, status string
			_ = attributevalue.Unmarshal(item["UploadId"], &uid)
			_ = attributevalue.Unmarshal(item["Status"], &status)
			if status == orphanStatus && uid != "" {
				orphanUploadIDs = append(orphanUploadIDs, uid)
			}
		}
	}

	if len(orphanUploadIDs) == 0 {
		return nil
	}

	log.WithFields(log.Fields{
		"manifest_id": manifestID,
		"count":       len(orphanUploadIDs),
	}).Info("resetFailedOrphans: flipping FailedOrphan rows back to Registered for retry")

	var firstErr error
	registered := manifestFile.Registered.String()
	for _, uid := range orphanUploadIDs {
		_, err := dy.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(fileTable),
			Key: map[string]dyTypes.AttributeValue{
				"ManifestId": &dyTypes.AttributeValueMemberS{Value: manifestID},
				"UploadId":   &dyTypes.AttributeValueMemberS{Value: uid},
			},
			// Re-assert the sparse InProgressIndex membership so manifest
			// completion accounting tracks this file again.
			UpdateExpression:    aws.String("SET #s = :new, InProgress = :x"),
			ConditionExpression: aws.String("#s = :orphan"),
			ExpressionAttributeNames: map[string]string{
				"#s": "Status",
			},
			ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
				":new":    &dyTypes.AttributeValueMemberS{Value: registered},
				":orphan": &dyTypes.AttributeValueMemberS{Value: orphanStatus},
				":x":      &dyTypes.AttributeValueMemberS{Value: "x"},
			},
		})
		if err != nil {
			var ccf *dyTypes.ConditionalCheckFailedException
			if errors.As(err, &ccf) {
				continue
			}
			log.WithError(err).WithFields(log.Fields{
				"manifest_id": manifestID,
				"upload_id":   uid,
			}).Warn("resetFailedOrphans: UpdateItem failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
