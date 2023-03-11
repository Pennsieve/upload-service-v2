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
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	pgdb2 "github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	testHelpers "github.com/pennsieve/pennsieve-go-core/pkg/test"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

type tesManifestFileParams struct {
	name  string
	path  string
	fType fileType.Type
}

// TestMain initializes test-suite
func TestMain(m *testing.M) {

	// If testing on Jenkins (-> DYNAMODB_URL is set) then wait for db to be active.
	if _, ok := os.LookupEnv("DYNAMODB_URL"); ok {
		time.Sleep(5 * time.Second)
	}
	ManifestTableName, _ = os.LookupEnv("MANIFEST_TABLE")
	ManifestFileTableName, _ = os.LookupEnv("MANIFEST_FILE_TABLE")
	SNSTopic = getEnv("IMPORTED_SNS_TOPIC", "dummy_sns_topic")

	var err error

	svc := getDynamoDBClient()
	_, _ = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(ManifestTableName)})
	_, _ = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(ManifestFileTableName)})

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
		TableName:   aws.String(ManifestTableName),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Printf("Couldn't create table. Here's why: %v\n", err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(svc)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(ManifestTableName)}, 5*time.Minute)
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
		TableName:   aws.String(ManifestFileTableName),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		log.Printf("Couldn't create table. Here's why: %v\n", err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(svc)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(ManifestFileTableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
	}

	// pre-populate manifest tables
	//orgId := 2
	pgdbClient, err := pgdb.ConnectENVWithOrg(2)
	if err != nil {
		log.Fatal("cannot connect to db:", err)
	}

	mSNS := test.MockSNS{}
	mS3 := test.MockS3{}
	client := getDynamoDBClient()
	store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, ManifestFileTableName, ManifestTableName, SNSTopic)

	err = populateManifest(store)
	if err != nil {
		log.Fatal("Unable to populate manifest.")
	}

	// close DB
	_ = pgdbClient.Close()

	// Run tests
	code := m.Run()

	// return
	os.Exit(code)
}

// TestUploadService iterates over and runs tests.
func TestUploadService(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadHandlerStore,
	){
		"correctly parsing the s3 events": testSQSMessageParser,
		"test pre-populated manifests":    testManifest,
		"test importing simple manifest":  testSimpleManifest,
		"test importing nested files":     testNestedManifest,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getDynamoDBClient()

			pgdbClient, err := pgdb.ConnectENV()
			if err != nil {
				log.Fatal("cannot connect to db:", err)
			}

			mSNS := test.MockSNS{}
			mS3 := test.MockS3{}
			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, ManifestFileTableName, ManifestTableName, SNSTopic)

			fn(t, store)
		})
	}
}

// TESTS

func testManifest(t *testing.T, store *UploadHandlerStore) {
	ctx := context.Background()
	m, err := store.dy.GetManifestsForDataset(ctx, ManifestTableName, "N:Dataset:1")
	assert.NoError(t, err)
	assert.Equal(t, len(m), 1)

}

func testSQSMessageParser(t *testing.T, store *UploadHandlerStore) {

	evts, err := getTestS3SQSEvents()
	assert.NoError(t, err)

	entries, _, err := store.GetUploadEntries(evts)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))

}

// HELPER FUNCTION
func populateManifest(store *UploadHandlerStore) error {

	ctx := context.Background()

	newManifest := dydb.ManifestTable{
		ManifestId:     "00000000-0000-0000-0000-000000000000",
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:1",
		OrganizationId: 2,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	}

	err := store.dy.CreateManifest(ctx, ManifestTableName, newManifest)
	if err != nil {
		return err
	}

	params := []tesManifestFileParams{
		{name: "file1.edf", path: "", fType: fileType.EDF},
		{name: "file2.edf", path: "", fType: fileType.EDF},
		{name: "file3.edf", path: "", fType: fileType.EDF},
		{name: "file4.edf", path: "", fType: fileType.EDF},
		{name: "file5.edf", path: "", fType: fileType.EDF},
	}
	files, _, _ := generateManifestFilesAndEvents(params, newManifest.ManifestId)

	fileStats := store.dy.AddFiles(newManifest.ManifestId, files, nil, ManifestFileTableName)
	if len(fileStats.FailedFiles) > 0 {
		return errors.New("could not pre-populate manifest files")
	}
	return nil
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

func generateManifestFilesAndEvents(params []tesManifestFileParams, manifestId string) ([]manifestFile.FileDTO, []events.SQSMessage, error) {

	var dtos []manifestFile.FileDTO
	var sqsMessages []events.SQSMessage
	for _, p := range params {
		uploadId, _ := uuid.NewUUID()
		dtos = append(dtos, manifestFile.FileDTO{
			UploadID:       uploadId.String(),
			TargetPath:     p.path,
			TargetName:     p.name,
			Status:         0,
			MergePackageId: "",
			FileType:       p.fType.String(),
		})

		evtRecord := events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{
					Name: "dummy-s3-bucket",
				},
				Object: events.S3Object{
					Key:  fmt.Sprintf("%s/%s", manifestId, uploadId),
					Size: 99,
					ETag: "fakeETag1",
				},
			},
		}
		evt := events.S3Event{Records: []events.S3EventRecord{evtRecord}}
		evtJson, err := json.Marshal(evt)
		if err != nil {
			return nil, nil, err
		}

		sqsMessage := events.SQSMessage{
			Body: string(evtJson),
		}

		sqsMessages = append(sqsMessages, sqsMessage)

	}

	return dtos, sqsMessages, nil

}

func getTestS3SQSEvents() ([]events.SQSMessage, error) {

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
	if err != nil {
		return nil, err
	}

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
	if err != nil {
		return nil, err
	}

	evts := []events.SQSMessage{
		{
			Body: string(eventJson1),
		},
		{
			Body: string(eventJson2),
		},
	}
	return evts, nil
}

func testSimpleManifest(t *testing.T, store *UploadHandlerStore) {

	orgId := 2
	defer func() {
		testHelpers.Truncate(t, store.pgdb, orgId, "packages")
		testHelpers.Truncate(t, store.pgdb, orgId, "files")
		testHelpers.Truncate(t, store.pgdb, orgId, "package_storage")
		testHelpers.Truncate(t, store.pgdb, orgId, "organization_storage")
		testHelpers.Truncate(t, store.pgdb, orgId, "dataset_storage")
	}()

	ctx := context.Background()
	newManifest := dydb.ManifestTable{
		ManifestId:     "00000000-0000-0000-0000-000000000000",
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:1",
		OrganizationId: 2,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	}

	params := []tesManifestFileParams{
		{name: "file1.edf", path: "", fType: fileType.EDF},
		{name: "file2.edf", path: "", fType: fileType.EDF},
		{name: "file3.edf", path: "", fType: fileType.EDF},
		{name: "file4.edf", path: "", fType: fileType.EDF},
		{name: "file5.edf", path: "", fType: fileType.EDF},
	}

	// Create Manifest
	err := store.dy.CreateManifest(ctx, ManifestTableName, newManifest)
	assert.NoError(t, err)

	// Create Manifest Files
	files, messages, _ := generateManifestFilesAndEvents(params, newManifest.ManifestId)
	_ = store.dy.AddFiles(newManifest.ManifestId, files, nil, ManifestFileTableName)

	// "Call" the Lambda function
	sqsEvents := events.SQSEvent{Records: messages}
	response, err := store.Handler(ctx, sqsEvents)
	assert.NoError(t, err)
	assert.Empty(t, response.BatchItemFailures)

	// Test entries that are created in the database.
	_ = store.WithOrg(int(newManifest.OrganizationId))
	packages, err := store.pg.GetPackageChildren(ctx, nil, int(newManifest.DatasetId), false)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(packages))

	allNamesCorrect := true

OUTER:
	for _, p := range packages {
		for _, e := range params {
			if p.Name == e.name {
				continue OUTER
			}
		}
		allNamesCorrect = false
		break OUTER
	}

	assert.True(t, allNamesCorrect, "Not all packages where returned or names correctly.")

}

func testNestedManifest(t *testing.T, store *UploadHandlerStore) {

	orgId := 2
	defer func() {
		testHelpers.Truncate(t, store.pgdb, orgId, "packages")
		testHelpers.Truncate(t, store.pgdb, orgId, "files")
		testHelpers.Truncate(t, store.pgdb, orgId, "package_storage")
		testHelpers.Truncate(t, store.pgdb, orgId, "organization_storage")
		testHelpers.Truncate(t, store.pgdb, orgId, "dataset_storage")
	}()

	ctx := context.Background()
	newManifest := dydb.ManifestTable{
		ManifestId:     "00000000-0000-0000-0000-000000000001",
		DatasetId:      1,
		DatasetNodeId:  "N:dataset:149b65da-6803-4a67-bf20-83076774a5c7",
		OrganizationId: 2,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	}

	params := []tesManifestFileParams{
		{path: "protocol_1/protocol_2", name: "Readme.md"},
		{path: "", name: "manifest.xlsx"},
		{path: "protocol_1", name: "manifest.xlsx"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5", name: "manifest.xlsx"},
		{path: "protocol_1/protocol_2", name: "manifest.xlsx"},
		{path: "protocol_1/protocol_2/protocol_3", name: "manifest.xlsx"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4", name: "Readme.md"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4", name: "manifest.xlsx"},
		{path: "", name: "Readme.md"},
		{path: "protocol_1", name: "Readme.md"},
		{path: "protocol_1/protocol_2/protocol_3", name: "Readme.md"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5", name: "Readme.md"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5/protocol_6", name: "Readme.md"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5/protocol_6", name: "manifest.xlsx"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5/protocol_6/protocol_7", name: "Readme.md"},
		{path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5/protocol_6/protocol_7", name: "manifest.xlsx"},
	}

	// Create Manifest
	err := store.dy.CreateManifest(ctx, ManifestTableName, newManifest)
	assert.NoError(t, err)

	// Create Manifest Files
	files, messages, _ := generateManifestFilesAndEvents(params, newManifest.ManifestId)
	_ = store.dy.AddFiles(newManifest.ManifestId, files, nil, ManifestFileTableName)

	// "Call" the Lambda function
	sqsEvents := events.SQSEvent{Records: messages}
	response, err := store.Handler(ctx, sqsEvents)
	assert.NoError(t, err)
	assert.Empty(t, response.BatchItemFailures)

	// Test entries that are created in the database.
	_ = store.WithOrg(int(newManifest.OrganizationId))

	//printPackageDetails(context.Background(), store, nil, int(newManifest.DatasetId))

	//Check that all packages in dataset are present in manifest
	//and all packages in manifest are present in dataset
	checkCreatedPackages(t, store, params, int(newManifest.DatasetId))

}

func checkCreatedPackages(t *testing.T, store *UploadHandlerStore, expected []tesManifestFileParams, datasetId int) {
	ctx := context.Background()
	fullPathMap := map[string]tesManifestFileParams{}
	for _, e := range expected {
		if e.path == "" {
			fullPathMap[fmt.Sprintf("%s", e.name)] = e
		} else {
			fullPathMap[fmt.Sprintf("%s/%s", e.path, e.name)] = e
		}
	}

	existMap := map[string]bool{}
	for key := range fullPathMap {
		existMap[key] = false
	}

	currentPath := ""
	existMap = checkPackageDetails(ctx, t, store, currentPath, nil, datasetId, fullPathMap, existMap)

	for key := range existMap {
		assert.True(t, existMap[key])
	}

}

func checkPackageDetails(ctx context.Context, t *testing.T, store *UploadHandlerStore, currentPath string, parent *pgdb2.Package,
	datasetId int, fullPathMap map[string]tesManifestFileParams, existMap map[string]bool) map[string]bool {

	// Check all packages in the database are part of expected list
	packages, _ := store.pg.GetPackageChildren(ctx, parent, datasetId, false)

	for _, p := range packages {
		var curFullPath string
		if currentPath == "" {
			curFullPath = fmt.Sprintf("%s", p.Name)
		} else {
			curFullPath = fmt.Sprintf("%s/%s", currentPath, p.Name)
		}

		if p.PackageType == packageType.Collection {
			existMap = checkPackageDetails(ctx, t, store, curFullPath, &p, datasetId, fullPathMap, existMap)
		} else {
			assert.Contains(t, fullPathMap, curFullPath)
			existMap[curFullPath] = true
		}
	}

	return existMap

}

//goland:noinspection GoUnusedFunction
func printPackageDetails(ctx context.Context, store *UploadHandlerStore, parent *pgdb2.Package, datasetId int) {

	fmt.Println("PRINTING PACKAGE DETAILS")
	fmt.Println("ROOT")
	packages, _ := store.pg.GetPackageChildren(ctx, parent, datasetId, false)
	for _, p := range packages {
		fmt.Println(fmt.Sprintf("Name: %s, Path: %d Type:%s", p.Name, p.ParentId.Int64, p.PackageType.String()))
		if p.PackageType == packageType.Collection {
			fmt.Println("FOLDER: ", p.Name)
			printPackageDetails(context.Background(), store, &p, datasetId)
		}
	}
}
