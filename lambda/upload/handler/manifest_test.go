package handler

import (
	"context"
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/dynamodb/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const manifestTableName = "upload-table"
const manifestFileTableName = "upload-file-table"

var testDB *sql.DB
var orgId int

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

func TestMain(m *testing.M) {

	// If testing on Jenkins (-> DYNAMODB_URL is set) then wait for db to be active.
	if _, ok := os.LookupEnv("DYNAMODB_URL"); ok {
		time.Sleep(5 * time.Second)
	}

	var err error

	orgId = 2
	//testDB, err = pgdb.ConnectENVWithOrg(orgId)
	//if err != nil {
	//	log.Fatal("cannot connect to db:", err)
	//}

	svc := getClient()
	svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String("upload-table")})
	svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String("upload-file-table")})

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
			{
				AttributeName: aws.String("UserId"),
				KeyType:       types.KeyTypeRange,
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
		TableName:   aws.String("upload-table"),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Printf("Couldn't create table. Here's why: %v\n", err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(svc)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String("upload-table")}, 5*time.Minute)
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
		TableName:   aws.String("upload-file-table"),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Printf("Couldn't create table. Here's why: %v\n", err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(svc)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String("upload-file-table")}, 5*time.Minute)
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
		tt *testing.T, store *UploadHandlerStore,
	){
		"create and get upload": testCreateGetManifest,
		"Add files to upload":   testAddFiles,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getClient()

			store := NewUploadHandlerStore(testDB, client, nil, manifestFileTableName, manifestTableName)

			fn(t, store)
		})
	}
}

func testCreateGetManifest(t *testing.T, store *UploadHandlerStore) {

	tb := models.ManifestTable{
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
	tb2 := models.ManifestTable{
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
	tb3 := models.ManifestTable{
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

func testAddFiles(t *testing.T, store *UploadHandlerStore) {

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
	manifestId := "1111"
	result := store.dy.AddFiles(manifestId, testFileDTOs, nil, store.fileTableName)

	// Checking returned status
	// Checking returned status
	assert.Equal(t, manifestFile.Unknown, result.FileStatus[0].Status)

	resultUploadIds := map[string]any{}
	for _, f := range result.FileStatus {
		resultUploadIds[f.UploadId] = nil
	}
	assert.Equal(t, testFileUploadIds, resultUploadIds)

}

//func testGetAction(t *testing.T, svc *dynamodb.Client) {
//
//	getAction(manifestId string, file manifestFile.FileDTO, curStatus manifestFile.Status)
//
//}
