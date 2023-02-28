package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/smithy-go/middleware"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const manifestTableName = "upload-table"
const manifestFileTableName = "upload-file-table"

type mockSNS struct{}

func (s mockSNS) PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	result := sns.PublishBatchOutput{
		Failed:         nil,
		Successful:     nil,
		ResultMetadata: middleware.Metadata{},
	}
	return &result, nil
}

type mockS3 struct{}

func (s mockS3) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	result := s3.HeadObjectOutput{
		ChecksumSHA256: aws.String("fakeSHA"),
	}

	return &result, nil
}

func testSQSMessageParser(t *testing.T, store *UploadHandlerStore) {
	eventRecord1 := events.S3EventRecord{
		S3: events.S3Entity{
			Bucket: events.S3Bucket{
				Name: "pennsieve-dev-uploads-v2-use1",
			},
			Object: events.S3Object{
				Key:  "00000000-0000-0000-0000-000000000000/00000000-1111-1111-1111-000000000000",
				Size: 99,
				ETag: "fakeETag1",
			},
		},
	}
	event1 := events.S3Event{Records: []events.S3EventRecord{eventRecord1}}
	eventJson1, err := json.Marshal(event1)
	assert.NoError(t, err)

	eventRecord2 := events.S3EventRecord{
		S3: events.S3Entity{
			Bucket: events.S3Bucket{
				Name: "pennsieve-dev-uploads-v2-use1",
			},
			Object: events.S3Object{
				Key:  "00000000-0000-0000-0000-000000000000/00000000-1111-1111-1111-000000000001",
				Size: 99,
				ETag: "fakeETag2",
			},
		},
	}

	event2 := events.S3Event{Records: []events.S3EventRecord{eventRecord2}}
	eventJson2, err := json.Marshal(event2)
	assert.NoError(t, err)

	events := []events.SQSMessage{
		{
			Body: string(eventJson1),
		},
		{
			Body: string(eventJson2),
		},
	}

	entries, err := store.GetUploadEntries(events)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))
	fmt.Println(entries)

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getDynamoDBClient() *dynamodb.Client {

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

	svc := getDynamoDBClient()
	_, _ = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String("upload-table")})
	_, _ = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String("upload-file-table")})

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

	// pre-populate manifest tables
	orgId := 2
	pgdbClient, err := pgdb.ConnectENVWithOrg(orgId)
	if err != nil {
		log.Fatal("cannot connect to db:", err)
	}

	mSNS := mockSNS{}
	mS3 := mockS3{}
	client := getDynamoDBClient()
	store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, manifestFileTableName, manifestTableName)

	populateManifest(store)

	// Run tests
	code := m.Run()

	// return
	os.Exit(code)
}

func TestUploadService(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadHandlerStore,
	){
		"correctly parsing the s3 events":       testSQSMessageParser,
		"test pre-populated manifests":          testManifest,
		"sorting of upload files":               testSorting,
		"test folder mapping from upload files": testGetUploadFolderMap,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getDynamoDBClient()

			orgId := 2
			pgdbClient, err := pgdb.ConnectENVWithOrg(orgId)
			if err != nil {
				log.Fatal("cannot connect to db:", err)
			}

			mSNS := mockSNS{}
			mS3 := mockS3{}
			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, manifestFileTableName, manifestTableName)

			fn(t, store)
		})
	}
}

func testManifest(t *testing.T, store *UploadHandlerStore) {
	ctx := context.Background()
	manifest, err := store.dy.GetManifestsForDataset(ctx, manifestTableName, "N:Dataset:1")
	assert.NoError(t, err)
	assert.Equal(t, len(manifest), 1)

}

func testSorting(t *testing.T, _ *UploadHandlerStore) {

	uploadFile1 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/asd/123/",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile2 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/asd/123",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}

	uploadFiles := []uploadFile.UploadFile{
		uploadFile1,
		uploadFile2,
	}

	var u uploadFile.UploadFile
	u.Sort(uploadFiles)
	assert.Equal(t, uploadFiles[0], uploadFile2)

}

func testGetUploadFolderMap(t *testing.T, _ *UploadHandlerStore) {

	uploadFile1 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/folder2/folder3",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile2 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/folder10",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile3 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder2/folder1/folder8",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}

	uploadFiles := []uploadFile.UploadFile{
		uploadFile1,
		uploadFile2,
		uploadFile3,
	}

	var u uploadFile.UploadFile
	folderMap := u.GetUploadFolderMap(uploadFiles, "")

	// Number of folders
	assert.Equal(t, 7, len(folderMap))

	// Check Folder exists
	assert.True(t, folderMap["folder1/folder10"] != nil)
	assert.True(t, folderMap["folder1/unknownFolder"] == nil)

	// Check Folder Parents
	assert.Equal(t, folderMap["folder1/folder10"].ParentNodeId, folderMap["folder1"].NodeId)
	assert.Equal(t, folderMap["folder1/folder2/folder3"].ParentNodeId, folderMap["folder1/folder2"].NodeId)
	assert.Equal(t, folderMap["folder1/folder10"].ParentNodeId, folderMap["folder1/folder2"].ParentNodeId)

	// Check folder depth
	assert.Equal(t, 0, folderMap["folder1"].Depth)
	assert.Equal(t, 2, folderMap["folder1/folder2/folder3"].Depth)

	// Check population of children in parents
	assert.Contains(t, folderMap["folder1"].Children, folderMap["folder1/folder10"])
	assert.Contains(t, folderMap["folder1"].Children, folderMap["folder1/folder2"])
	assert.NotContains(t, folderMap["folder1"].Children, folderMap["folder2/folder1"])

	//** Check with alternative root folder.

	folderMap2 := u.GetUploadFolderMap(uploadFiles, "hello/you")

	// Number of folders
	assert.Equal(t, 9, len(folderMap2))

	// Check Folder exists
	assert.True(t, folderMap2["hello/you/folder1/folder10"] != nil)
	assert.True(t, folderMap2["hello/you/folder1/unknownFolder"] == nil)

	// Check Folder Parents
	assert.Equal(t, folderMap2["hello/you/folder1/folder10"].ParentNodeId, folderMap2["hello/you/folder1"].NodeId)
	assert.Equal(t, folderMap2["hello/you/folder1/folder2/folder3"].ParentNodeId, folderMap2["hello/you/folder1/folder2"].NodeId)
	assert.Equal(t, folderMap2["hello/you/folder1/folder10"].ParentNodeId, folderMap2["hello/you/folder1/folder2"].ParentNodeId)

	// Check folder depth
	assert.Equal(t, 2, folderMap2["hello/you/folder1"].Depth)
	assert.Equal(t, 4, folderMap2["hello/you/folder1/folder2/folder3"].Depth)

}

func populateManifest(store *UploadHandlerStore) error {

	ctx := context.Background()

	newManifest := dydb.ManifestTable{
		ManifestId:     "1234567789",
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:1",
		OrganizationId: 2,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	}

	err := store.dy.CreateManifest(ctx, manifestTableName, newManifest)
	if err != nil {
		return err
	}

	files := []manifestFile.FileDTO{
		{
			UploadID:   "1",
			TargetPath: "",
			TargetName: "file1.edf",
			Status:     0,
			FileType:   fileType.EDF.String(),
		},
		{
			UploadID:   "2",
			TargetPath: "",
			TargetName: "file2.edf",
			Status:     0,
			FileType:   fileType.EDF.String(),
		},
		{
			UploadID:       "3",
			S3Key:          "",
			TargetPath:     "",
			TargetName:     "file3.zip",
			Status:         0,
			MergePackageId: "",
			FileType:       fileType.ZIP.String(),
		},
	}

	fileStats := store.dy.AddFiles(newManifest.ManifestId, files, nil, manifestFileTableName)
	if len(fileStats.FailedFiles) > 0 {
		return errors.New("could not pre-populate manifest files")
	}
	return nil
}

func testHandler(t *testing.T, store *UploadHandlerStore) {

	ctx := context.Background()
	messages := []events.SQSMessage{
		{
			MessageId:              "",
			ReceiptHandle:          "",
			Body:                   "",
			Md5OfBody:              "",
			Md5OfMessageAttributes: "",
			Attributes:             nil,
			MessageAttributes:      nil,
			EventSourceARN:         "",
			EventSource:            "aws:s3",
			AWSRegion:              "us-east-1",
		},
		{
			MessageId:              "",
			ReceiptHandle:          "",
			Body:                   "",
			Md5OfBody:              "",
			Md5OfMessageAttributes: "",
			Attributes:             nil,
			MessageAttributes:      nil,
			EventSourceARN:         "",
			EventSource:            "",
			AWSRegion:              "",
		},
		{
			MessageId:              "",
			ReceiptHandle:          "",
			Body:                   "",
			Md5OfBody:              "",
			Md5OfMessageAttributes: "",
			Attributes:             nil,
			MessageAttributes:      nil,
			EventSourceARN:         "",
			EventSource:            "",
			AWSRegion:              "",
		},
	}

	sqsEvents := events.SQSEvent{Records: messages}
	Handler(ctx, sqsEvents)

}
