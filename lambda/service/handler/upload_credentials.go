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
	log "github.com/sirupsen/logrus"
)

type uploadCredentialsRequest struct {
	ManifestNodeID string `json:"manifestNodeId"`
}

type uploadCredentialsResponse struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey  string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
	Expiration      string `json:"expiration"`
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
}

func postUploadCredentialsRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	var req uploadCredentialsRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 400,
			Body:       gateway.CreateErrorMessage("Invalid request body: "+err.Error(), 400),
		}, nil
	}

	if req.ManifestNodeID == "" {
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 400,
			Body:       gateway.CreateErrorMessage("manifestNodeId is required", 400),
		}, nil
	}

	uploadRoleARN := os.Getenv("UPLOAD_CREDENTIALS_ROLE_ARN")
	if uploadRoleARN == "" {
		log.Error("UPLOAD_CREDENTIALS_ROLE_ARN not configured")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Upload credentials not configured", 500),
		}, nil
	}

	uploadBucket := os.Getenv("UPLOAD_BUCKET")
	if uploadBucket == "" {
		log.Error("UPLOAD_BUCKET not configured")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Upload bucket not configured", 500),
		}, nil
	}

	region := os.Getenv("REGION")

	// Scope credentials to only allow writes under this manifest's prefix
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
	}`, uploadBucket, uploadBucket, req.ManifestNodeID)

	sessionName := fmt.Sprintf("upload-%d-%d", claims.OrgClaim.IntId, claims.UserClaim.Id)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.WithError(err).Error("Failed to load AWS config")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Internal error", 500),
		}, nil
	}

	stsClient := sts.NewFromConfig(cfg)
	durationSeconds := int32(3600) // 1 hour
	result, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(uploadRoleARN),
		RoleSessionName: aws.String(sessionName),
		Policy:          aws.String(sessionPolicy),
		DurationSeconds: &durationSeconds,
	})
	if err != nil {
		log.WithError(err).Error("Failed to assume upload role")
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       gateway.CreateErrorMessage("Failed to generate upload credentials", 500),
		}, nil
	}

	resp := uploadCredentialsResponse{
		AccessKeyID:    *result.Credentials.AccessKeyId,
		SecretAccessKey: *result.Credentials.SecretAccessKey,
		SessionToken:   *result.Credentials.SessionToken,
		Expiration:     result.Credentials.Expiration.Format(time.RFC3339),
		Bucket:         uploadBucket,
		Region:         region,
	}

	jsonBody, _ := json.Marshal(resp)
	return &events.APIGatewayV2HTTPResponse{
		StatusCode: 200,
		Body:       string(jsonBody),
	}, nil
}