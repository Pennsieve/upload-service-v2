package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/gateway"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-upload-service-v2/service/pkg/storage"
	log "github.com/sirupsen/logrus"
)

type storageCredentialsRequest struct {
	ManifestNodeID string `json:"manifestNodeId"`
}

type storageCredentialsResponse struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
	Expiration      string `json:"expiration"`
	Bucket          string `json:"bucket"`
	KeyPrefix       string `json:"keyPrefix"`
	Region          string `json:"region"`
}

// postStorageCredentialsRoute returns STS credentials scoped to the manifest's
// destination storage bucket + O{org}/D{dataset}/{manifest}/* prefix, so the
// agent can upload directly to final storage (no intermediate upload bucket).
func postStorageCredentialsRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	var req storageCredentialsRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 400,
			Body:       gateway.CreateErrorMessage("Invalid request body: "+err.Error(), 400),
		}, nil
	}
	if !isValidUUID(req.ManifestNodeID) {
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 400,
			Body:       gateway.CreateErrorMessage("manifestNodeId must be a UUID", 400),
		}, nil
	}

	ctx := context.Background()

	manifestRecord, err := store.dy.GetManifestById(ctx, store.tableName, req.ManifestNodeID)
	if err != nil {
		log.WithError(err).WithField("manifestNodeId", req.ManifestNodeID).Warn("manifest not found")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 404,
			Body:       gateway.CreateErrorMessage("Manifest not found", 404),
		}, nil
	}
	if manifestRecord.DatasetNodeId != claims.DatasetClaim.NodeId {
		log.WithFields(log.Fields{
			"manifestDataset": manifestRecord.DatasetNodeId,
			"claimsDataset":   claims.DatasetClaim.NodeId,
		}).Warn("manifest does not belong to the authenticated dataset")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 403,
			Body:       gateway.CreateErrorMessage("Manifest does not belong to this dataset", 403),
		}, nil
	}

	roleARN := os.Getenv("STORAGE_CREDENTIALS_ROLE_ARN")
	if roleARN == "" {
		log.Error("STORAGE_CREDENTIALS_ROLE_ARN not configured")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Storage credentials not configured", 500),
		}, nil
	}
	defaultStorageBucket := os.Getenv("DEFAULT_STORAGE_BUCKET")
	if defaultStorageBucket == "" {
		log.Error("DEFAULT_STORAGE_BUCKET not configured")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Storage not configured", 500),
		}, nil
	}
	region := os.Getenv("REGION")

	// Resolve destination bucket via shared resolver (workspace-scoped today,
	// per-dataset in the future).
	pgdb, err := pgQueries.ConnectRDS()
	if err != nil {
		log.WithError(err).Error("failed to connect to RDS")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Internal error", 500),
		}, nil
	}
	defer pgdb.Close()

	resolution, err := storage.ResolveForManifest(
		ctx,
		req.ManifestNodeID,
		store.tableName,
		defaultStorageBucket,
		dyQueries.New(store.dynamodb),
		pgQueries.New(pgdb),
	)
	if err != nil {
		log.WithError(err).WithField("manifestNodeId", req.ManifestNodeID).Error("failed to resolve storage bucket")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Failed to resolve storage bucket", 500),
		}, nil
	}

	// Future: if resolver returns a non-S3 backend (Azure, local), return 409 so
	// the agent can fall back or use a backend-specific auth path. Today all
	// backends are S3, so this is a no-op until the resolver learns about
	// multiple backend types.
	// if resolution.Backend != storage.BackendS3 { ... return 409 ... }

	sessionPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:ListBucketMultipartUploads",
				"s3:AbortMultipartUpload",
				"s3:ListMultipartUploadParts",
				"s3:PutObjectTagging"
			],
			"Resource": [
				"arn:aws:s3:::%s",
				"arn:aws:s3:::%s/%s/*"
			]
		}]
	}`, resolution.StorageBucket, resolution.StorageBucket, resolution.KeyPrefix(req.ManifestNodeID))

	sessionName := fmt.Sprintf("storage-%d-%d", claims.OrgClaim.IntId, claims.UserClaim.Id)

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.WithError(err).Error("Failed to load AWS config")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Internal error", 500),
		}, nil
	}

	stsClient := sts.NewFromConfig(cfg)
	durationSeconds := int32(3600)
	result, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName),
		Policy:          aws.String(sessionPolicy),
		DurationSeconds: &durationSeconds,
	})
	if err != nil {
		log.WithError(err).Error("Failed to assume storage credentials role")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Failed to generate storage credentials", 500),
		}, nil
	}

	resp := storageCredentialsResponse{
		AccessKeyID:     *result.Credentials.AccessKeyId,
		SecretAccessKey: *result.Credentials.SecretAccessKey,
		SessionToken:    *result.Credentials.SessionToken,
		Expiration:      result.Credentials.Expiration.Format(time.RFC3339),
		Bucket:          resolution.StorageBucket,
		KeyPrefix:       resolution.KeyPrefix(req.ManifestNodeID),
		Region:          region,
	}

	jsonBody, _ := json.Marshal(resp)
	return &events.APIGatewayV2HTTPResponse{
		StatusCode: 200,
		Body:       string(jsonBody),
	}, nil
}
