package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload-move-files/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func TestGetRegionalS3Client(t *testing.T) {

	storageOrgItemUSE1 := &storageOrgItem{
		organizationId: 0,
		storageBucket:  "pennsieve-storage-use1",
		datasetId:      0,
	}
	storageOrgItemAFS1 := &storageOrgItem{
		organizationId: 0,
		storageBucket:  "pennsieve-storage-afs1",
		datasetId:      0,
	}

	storageOrgItemMES1 := &storageOrgItem{
		organizationId: 0,
		storageBucket:  "pennsieve-storage-mes1",
		datasetId:      0,
	}

	storageOrgItemNoShortName := &storageOrgItem{
		organizationId: 0,
		storageBucket:  "pennsieve-storage",
		datasetId:      0,
	}

	storageOrgItemNoRegion := &storageOrgItem{
		organizationId: 0,
		storageBucket:  "pennsieve-storage-ufc1",
		datasetId:      0,
	}

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

	s3Client, expectedUSE1Region, err := pkg.CreateClient(storageOrgItemUSE1.storageBucket)
	assert.Nil(t, err)
	assert.IsType(t, &s3.Client{}, s3Client)

	_, expectedAFS1Region, err := pkg.CreateClient(storageOrgItemAFS1.storageBucket)
	assert.Nil(t, err)

	_, expectedMES1Region, err := pkg.CreateClient(storageOrgItemMES1.storageBucket)
	assert.Nil(t, err)

	_, _, expectedError := pkg.CreateClient(storageOrgItemNoShortName.storageBucket)
	assert.Error(t, expectedError)

	_, _, expectedError = pkg.CreateClient(storageOrgItemNoRegion.storageBucket)
	assert.Error(t, expectedError)

	assert.Equal(t, pkg.AWSRegions["use1"], expectedUSE1Region)
	assert.Equal(t, pkg.AWSRegions["afs1"], expectedAFS1Region)
	assert.Equal(t, pkg.AWSRegions["mes1"], expectedMES1Region)

}

func TestGetRegion(t *testing.T) {

	USE1Region, exists := pkg.GetRegion("pennsieve-storage-use1")
	assert.True(t, exists)
	AFS1Region, exists := pkg.GetRegion("pennsieve-storage-afs1")
	assert.True(t, exists)
	EUW1Region, exists := pkg.GetRegion("pennsieve-storage-euw1")
	assert.True(t, exists)
	APS2Region, exists := pkg.GetRegion("pennsieve-storage-aps2")
	assert.True(t, exists)
	USE1Region2, exists := pkg.GetRegion("PENNSIEVE-STORAGE-USE1")
	assert.True(t, exists)
	_, exists = pkg.GetRegion("PENNSIEVE-STORAGE-DEV")
	assert.False(t, exists)

	assert.Equal(t, pkg.AWSRegions["use1"], USE1Region)
	assert.Equal(t, pkg.AWSRegions["afs1"], AFS1Region)
	assert.Equal(t, pkg.AWSRegions["euw1"], EUW1Region)
	assert.Equal(t, pkg.AWSRegions["aps2"], APS2Region)
	assert.Equal(t, pkg.AWSRegions["use1"], USE1Region2)

}
