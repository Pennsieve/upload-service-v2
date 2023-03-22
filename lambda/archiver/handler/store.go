package handler

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	log "github.com/sirupsen/logrus"
	"os"
)

// ArchiverStore provides the Queries interface.
type ArchiverStore struct {
	*dydb.Queries
	dynamodb      *dynamodb.Client
	s3Client      *s3.Client
	fileTableName string
	tableName     string
}

// NewArchiverStore returns a ArchiverStore object which implements the Queires
func NewArchiverStore(dy *dynamodb.Client, s3Client *s3.Client, fileTableName string, tableName string) *ArchiverStore {
	return &ArchiverStore{
		dynamodb:      dy,
		s3Client:      s3Client,
		Queries:       dydb.New(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}

func (s *ArchiverStore) writeCSVFile(ctx context.Context, fileName string, manifestId string) (string, error) {
	file, err := os.Create(fmt.Sprintf("/tmp/%s", fileName))
	defer file.Close()
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
			}).Error("unable to create archive CSV file")
		return file.Name(), err

	}

	w := csv.NewWriter(file)
	defer w.Flush()

	files, _, err := s.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, 1000, nil)
	if err != nil {
		return file.Name(), err
	}

	// Write Headers
	firstFile := files[0]
	headers := firstFile.GetHeaders()
	err = w.Write(headers)
	if err != nil {
		return file.Name(), err
	}

	for _, f := range files {
		rowSlice := f.ToSlice()
		err := w.Write(rowSlice)
		if err != nil {
			return file.Name(), err
		}
	}

	return file.Name(), nil

}

func (s *ArchiverStore) writeManifestToS3(ctx context.Context, fileName string, organizationId int64, datasetId int64) (string, error) {

	archiverS3Key := fmt.Sprintf("O%d/D%d/%s", organizationId, datasetId, fileName)
	filePath := fmt.Sprintf("/tmp/%s", fileName)
	// open file for reading
	uploadFile, err := os.Open(filePath)
	if err != nil {
		return "", err
	}

	uploader := manager.NewUploader(store.s3Client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(archiverBucket),
		Key:    aws.String(archiverS3Key),
		Body:   uploadFile,
	})
	if err != nil {
		return "", err
	}

	return archiverS3Key, nil
}
