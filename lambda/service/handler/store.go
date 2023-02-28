package handler

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
)

// UploadServiceStore provides the Queries interface and a db instance.
type UploadServiceStore struct {
	*dydb.Queries
	dynamodb      *dynamodb.Client
	fileTableName string
	tableName     string
}

// NewUploadHandlerStore returns a UploadHandlerStore object which implements the Queires
func NewUploadServiceStore(dy *dynamodb.Client, fileTableName string, tableName string) *UploadServiceStore {
	return &UploadServiceStore{
		dynamodb:      dy,
		Queries:       dydb.New(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}
