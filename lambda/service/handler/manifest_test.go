package handler

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-upload-service-v2/service/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const manifestTableName = "upload-table"
const manifestFileTableName = "upload-file-table"

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getClient() *dynamodb.Client {

	testDBUri := getEnv("DYNAMODB_URL", "http://localhost:8000")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy_secret", "1234")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: testDBUri}, nil
			})),
	)
	if err != nil {
		panic(err)
	}

	svc := dynamodb.NewFromConfig(cfg)
	return svc
}

func getS3Client() *s3.Client {

	testDBUri := getEnv("MINIO_URL", "http://localhost:9002")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: testDBUri, HostnameImmutable: true}, nil
			})),
	)
	if err != nil {
		log.Error("Cannot create Minio resource")
		panic(err)
	}

	s3Client := s3.NewFromConfig(cfg)

	return s3Client

}

func TestMain(m *testing.M) {

	// If testing on Jenkins (-> DYNAMODB_URL is set) then wait for db to be active.
	if _, ok := os.LookupEnv("DYNAMODB_URL"); ok {
		time.Sleep(5 * time.Second)
	}

	var err error

	//testDB, err = pgdb.ConnectENVWithOrg(orgId)
	//if err != nil {
	//	log.Fatal("cannot connect to db:", err)
	//}

	svc := getClient()
	_, _ = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(manifestTableName)})
	_, _ = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(manifestFileTableName)})

	_, err = svc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ManifestId"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("UserId"),
				AttributeType: types.ScalarAttributeTypeN,
			},
			{
				AttributeName: aws.String("DatasetNodeId"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ManifestId"),
				KeyType:       types.KeyTypeHash,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("DatasetManifestIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("DatasetNodeId"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("UserId"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   "ALL",
				},
				ProvisionedThroughput: nil,
			},
		},
		TableName:   aws.String(manifestTableName),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Printf("Couldn't create table. Here's why: %v\n", err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(svc)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(manifestTableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
	}

	_, err = svc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ManifestId"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("UploadId"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("Status"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("FilePath"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("InProgress"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ManifestId"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("UploadId"),
				KeyType:       types.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("StatusIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("Status"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("ManifestId"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					NonKeyAttributes: []string{"ManifestId", "UploadId", "FileName", "FilePath", "FileType"},
					ProjectionType:   types.ProjectionTypeInclude,
				},
				ProvisionedThroughput: nil,
			},
			{
				IndexName: aws.String("InProgressIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("ManifestId"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("InProgress"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					NonKeyAttributes: []string{"FileName", "FilePath", "FileType", "Status"},
					ProjectionType:   types.ProjectionTypeInclude,
				},
				ProvisionedThroughput: nil,
			},
			{
				IndexName: aws.String("PathIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("ManifestId"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("FilePath"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					NonKeyAttributes: []string{"FileName", "UploadId", "MergePackageId"},
					ProjectionType:   types.ProjectionTypeInclude,
				},
				ProvisionedThroughput: nil,
			},
		},
		TableName:   aws.String(manifestFileTableName),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Printf("Couldn't create table. Here's why: %v\n", err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(svc)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(manifestFileTableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
	}

	// Run tests
	code := m.Run()

	// return
	os.Exit(code)
}

func TestManifest(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadServiceStore,
	){
		"create and get upload": testCreateGetManifest,
		"Add files to upload":   testAddFiles,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getClient()

			s3Client := getS3Client()
			mockLambda := test.MockLambda{}

			store := NewUploadServiceStore(client, s3Client, &mockLambda, manifestFileTableName, manifestTableName)

			fn(t, store)
		})
	}
}

func testCreateGetManifest(t *testing.T, store *UploadServiceStore) {

	tb := dydb.ManifestTable{
		ManifestId:     "1111",
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:1234",
		OrganizationId: 1,
		UserId:         1,
		Status:         "Unknown",
		DateCreated:    time.Now().Unix(),
	}

	// Create Manifest
	ctx := context.Background()
	err := store.dy.CreateManifest(ctx, manifestTableName, tb)
	assert.Nil(t, err, "Manifest 1 could not be created")

	// Create second upload
	tb2 := dydb.ManifestTable{
		ManifestId:     "2222",
		DatasetId:      2,
		DatasetNodeId:  "N:Dataset:5678",
		OrganizationId: 1,
		UserId:         1,
		Status:         "Unknown",
		DateCreated:    time.Now().Unix(),
	}

	err = store.dy.CreateManifest(ctx, manifestTableName, tb2)
	assert.Nil(t, err, "Manifest 2 could not be created")

	// Create second upload
	tb3 := dydb.ManifestTable{
		ManifestId:     "3333",
		DatasetId:      2,
		DatasetNodeId:  "N:Dataset:5678",
		OrganizationId: 1,
		UserId:         1,
		Status:         "Unknown",
		DateCreated:    time.Now().Unix(),
	}

	err = store.dy.CreateManifest(ctx, manifestTableName, tb3)
	assert.Nil(t, err, "Manifest 3 could not be created")

	// Get Manifest
	out, err := store.dy.GetManifestsForDataset(ctx, "upload-table", "N:Dataset:1234")
	assert.Nil(t, err, "Manifest could not be fetched")
	assert.Equal(t, 1, len(out))
	assert.Equal(t, "1111", out[0].ManifestId)
	assert.Equal(t, int64(1), out[0].OrganizationId)
	assert.Equal(t, int64(1), out[0].UserId)

	// Check that there are two manifests for N:Dataset:5678
	out, err = store.dy.GetManifestsForDataset(ctx, "upload-table", "N:Dataset:5678")
	assert.Nil(t, err, "Manifest could not be fetched")
	assert.Equal(t, 2, len(out))
	assert.Equal(t, "2222", out[0].ManifestId)
	assert.Equal(t, "3333", out[1].ManifestId)
}

func testAddFiles(t *testing.T, store *UploadServiceStore) {

	ctx := context.Background()
	manifestId := "0002"
	err := store.dy.CreateManifest(ctx, manifestTableName, dydb.ManifestTable{
		ManifestId:     manifestId,
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:0002",
		OrganizationId: 1,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	})
	assert.NoError(t, err)

	testFileDTOs := []manifestFile.FileDTO{
		{
			UploadID:       "111",
			S3Key:          "",
			TargetPath:     "folder1",
			TargetName:     "file1",
			Status:         manifestFile.Unknown,
			MergePackageId: "",
			FileType:       fileType.Aperio.String(),
		},
		{
			UploadID:       "222",
			S3Key:          "",
			TargetPath:     "folder1",
			TargetName:     "file2",
			Status:         manifestFile.Unknown,
			MergePackageId: "",
			FileType:       fileType.Aperio.String(),
		},
	}

	testFileUploadIds := map[string]any{}
	for _, f := range testFileDTOs {
		testFileUploadIds[f.UploadID] = nil
	}

	// Adding files to upload
	result, err := store.dy.SyncFiles(manifestId, testFileDTOs, nil, store.tableName, store.fileTableName)
	assert.NoError(t, err)

	// Checking returned status
	assert.Equal(t, manifestFile.Unknown, result.FileStatus[0].Status)

	resultUploadIds := map[string]any{}
	for _, f := range result.FileStatus {
		resultUploadIds[f.UploadId] = nil
	}
	assert.Equal(t, testFileUploadIds, resultUploadIds)

}
