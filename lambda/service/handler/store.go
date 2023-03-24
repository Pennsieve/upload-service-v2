package handler

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/pennsieve-go-core/pkg/domain"
)

// UploadServiceStore provides the Queries interface and a db instance.
type UploadServiceStore struct {
	dy            *ServiceDyQueries
	dynamodb      *dynamodb.Client
	s3Client      domain.S3API
	lambdaClient  LambdaAPI
	fileTableName string
	tableName     string
}

// NewUploadServiceStore returns a UploadHandlerStore object which implements the Queires
func NewUploadServiceStore(dy *dynamodb.Client, s3Client domain.S3API, lambdaClient LambdaAPI, fileTableName string, tableName string) *UploadServiceStore {
	return &UploadServiceStore{
		dynamodb:      dy,
		s3Client:      s3Client,
		lambdaClient:  lambdaClient,
		dy:            NewServiceDyQueries(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}

//func (s *UploadServiceStore) archiver(manifestId string) {
//
//	// Check
//
//
//	// Create temporary
//	//time.Now().String()
//	//csvFileName := fmt.Sprintf("manifest_archive_%s.csv", manifestId)
//	//
//	//file, err := os.Create(fmt.Sprintf("/tmp/%s", csvFileName))
//	//defer file.Close()
//	//if err != nil {
//	//	log.Fatalln("failed to open file", err)
//	//}
//	//
//	//w := csv.NewWriter(file)
//	//
//	//defer w.Flush()
//	//
//	//csvWriter := csv.NewWriter()
//	//
//	//csvWriter.Write()
//
//}
