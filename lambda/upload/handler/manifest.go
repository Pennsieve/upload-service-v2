package handler

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	core2 "github.com/pennsieve/pennsieve-go-core/pkg/domain"
)

//
//var syncWG sync.WaitGroup
//
//const batchSize = 25 // maximum batch size for batchPut action on dynamodb
//const nrWorkers = 2  // preliminary profiling shows that more workers don't improve efficiency for up to 1000 files

type ManifestSession struct {
	FileTableName string
	TableName     string
	Client        *dynamodb.Client
	SNSClient     core2.SnsAPI
	SNSTopic      string
	S3Client      core2.S3API
}
