package handler

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/test"
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
	// Initialize logger
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.JSONFormatter{})

	// If testing on Jenkins (-> DYNAMODB_URL is set) then wait for db to be active.
	if _, ok := os.LookupEnv("DYNAMODB_URL"); ok {
		time.Sleep(5 * time.Second)
	}

	svc := getClient()
	test.SetupDynamoDB(svc, manifestTableName, manifestFileTableName)
	archiverBucket = "test-archiever-bucket"

	s3Client := getS3Client()
	s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket:              aws.String(archiverBucket),
		ExpectedBucketOwner: nil,
	})

	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(archiverBucket),
	})
	if err != nil {
		log.Info("Bucket already exists: ", err)
	}

	// Run tests
	code := m.Run()

	// return
	os.Exit(code)

}

func TestArchiver(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T,
	){
		"write manifest to CSV file":               testWriteManifestCsv,
		"write CSV to S3":                          testWriteCSVToS3,
		"remove rows from file-table for manifest": testRemoveManifestFiles,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getClient()
			s3Client := getS3Client()

			store = NewArchiverStore(client, s3Client, manifestFileTableName, manifestTableName)

			fn(t)
		})
	}
}

func testWriteManifestCsv(t *testing.T) {
	// Create manifest and populate in DynamoDB
	ctx := context.Background()
	manifestId := "Manifest:0001"
	err := populateManifest(ctx, store, manifestId)
	assert.NoError(t, err)

	// Write manifest to CSV
	exportFileName := "exported_manifest.csv"
	filePath, err := store.writeCSVFile(ctx, exportFileName, manifestId)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("/tmp/%s", exportFileName), filePath)

	// Read CSV and confirm the entries
	f, err := os.Open(fmt.Sprintf("/tmp/%s", exportFileName))
	assert.NoError(t, err)
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	assert.NoError(t, err)
	assert.Len(t, lines, 3, "Manifest CSV should have 1 header row and 2 entries")

	// Check headers
	headers := lines[0]
	mt := dydb.ManifestFileTable{}
	expectedHeaders := mt.GetHeaders()
	for i, h := range expectedHeaders {
		assert.Equal(t, h, headers[i], fmt.Sprintf("Headers in exported file should match dynamodb headers"))
	}

	// Check values
	assert.Equal(t, "Manifest:0001", lines[1][0], "First column of csv should be manifest-id")
	assert.Equal(t, "Manifest:0001-1", lines[1][1], "Second column of csv should be upload-id")
	assert.Equal(t, "Manifest:0001-2", lines[2][1], "Second column of csv should be upload-id")

}

func testRemoveManifestFiles(t *testing.T) {

	//manifestId := "0002"
	ctx := context.Background()
	manifestId := "Manifest:0003"
	err := store.dy.CreateManifest(ctx, manifestTableName, dydb.ManifestTable{
		ManifestId:     manifestId,
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:0002",
		OrganizationId: 1,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	})
	assert.NoError(t, err, "should be able to create manifest")

	// Create 100 fileDTOs
	var testFileDTOs []manifestFile.FileDTO
	i := 1
	for i <= 100 {
		testFileDTOs = append(testFileDTOs, manifestFile.FileDTO{
			UploadID:       fmt.Sprintf("%s-%d", manifestId, i),
			S3Key:          "",
			TargetPath:     "",
			TargetName:     fmt.Sprintf("%s-%d", manifestId, i),
			Status:         manifestFile.Local,
			MergePackageId: "",
			FileType:       fileType.Aperio.String(),
		})
		i++
	}

	// Adding files to upload
	stat, err := store.dy.SyncFiles(manifestId, testFileDTOs, nil, store.tableName, store.fileTableName)
	assert.NoError(t, err, "Should be able to create manifest files.")
	assert.Equal(t, 100, stat.NrFilesUpdated, "Number of updated files should match input")

	// Check that files are there
	resFiles, _, err := store.dy.GetFilesPaginated(ctx, store.fileTableName, manifestId, sql.NullString{Valid: false}, 100, nil)
	assert.NoError(t, err, "shoudl be able to get files")
	assert.Len(t, resFiles, 100, "Should have 100 records")

	// This should remove all files
	err = store.removeManifestFiles(ctx, manifestId)
	assert.NoError(t, err, "Should be able to remove files.")

	// Try to get files from dynamoDB for manifest, which should be empty
	resFiles, next, err := store.dy.GetFilesPaginated(ctx, store.fileTableName, manifestId, sql.NullString{Valid: false}, 100, nil)
	assert.NoError(t, err, "Should be able to get paginated files even if no files exist.")
	assert.Len(t, next, 0, "Should not be any pointer to next data")
	assert.Len(t, resFiles, 0, "All files should be deleted")

}

func testWriteCSVToS3(t *testing.T) {

	// create CSV file
	exportFileName := "testCSV.csv"
	filePath := fmt.Sprintf("/tmp/%s", exportFileName)
	f, err := os.Create(filePath)
	assert.NoError(t, err)
	_, err = f.WriteString("This is a test")
	assert.NoError(t, err)
	f.Close()

	ctx := context.Background()
	orgId := int64(1)
	datasetId := int64(1)

	s3Key, err := store.writeManifestToS3(ctx, exportFileName, orgId, datasetId)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("O%d/D%d/%s", orgId, datasetId, exportFileName), s3Key)

	// Check if file is in S3
	_, err = store.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(archiverBucket),
		Key:    aws.String(s3Key),
	})
	assert.NoError(t, err)

}

func populateManifest(ctx context.Context, store *ArchiverStore, manifestId string) error {

	//manifestId := "0002"
	err := store.dy.CreateManifest(ctx, manifestTableName, dydb.ManifestTable{
		ManifestId:     manifestId,
		DatasetId:      1,
		DatasetNodeId:  "N:Dataset:0002",
		OrganizationId: 1,
		UserId:         1,
		Status:         manifest.Initiated.String(),
		DateCreated:    time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	testFileDTOs := []manifestFile.FileDTO{
		{
			UploadID:       fmt.Sprintf("%s-1", manifestId),
			S3Key:          "",
			TargetPath:     "folder1",
			TargetName:     "file1",
			Status:         manifestFile.Local,
			MergePackageId: "",
			FileType:       fileType.Aperio.String(),
		},
		{
			UploadID:       fmt.Sprintf("%s-2", manifestId),
			S3Key:          "",
			TargetPath:     "folder1",
			TargetName:     "file2",
			Status:         manifestFile.Local,
			MergePackageId: "",
			FileType:       fileType.Aperio.String(),
		},
	}

	testFileUploadIds := map[string]any{}
	for _, f := range testFileDTOs {
		testFileUploadIds[f.UploadID] = nil
	}

	// Adding files to upload
	_, err := store.dy.SyncFiles(manifestId, testFileDTOs, nil, store.tableName, store.fileTableName)
	if err != nil {
		return err
	}

	return nil
}
