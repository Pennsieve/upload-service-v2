package main

import (
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	dynamo2 "github.com/pennsieve/pennsieve-go-core/pkg/dynamodb"
	"github.com/pennsieve/pennsieve-go-core/pkg/pgdb"
)

// UploadMoveStore provides the Queries interface and a db instance.
type UploadMoveStore struct {
	pg *pgdb.Queries
	dy *dynamo2.Queries
	db *sql.DB
	s3 *s3.Client
}

// NewUploadMoveStore returns a NewUploadMoveStore object which implements the Queries
func NewUploadMoveStore(db *sql.DB, dydb *dynamodb.Client, s3 *s3.Client) *UploadMoveStore {
	return &UploadMoveStore{
		db: db,
		pg: pgdb.New(db),
		dy: dynamo2.New(dydb),
		s3: s3,
	}
}
