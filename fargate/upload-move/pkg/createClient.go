package pkg

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"strings"
)

// CreateClient creates a client for the appropriate region data is being copied to
func CreateClient(storageBucket string) (*s3.Client, AWSRegion, error) {

	region, exists := GetRegion(storageBucket)
	if !exists {
		return nil, AWSRegion{}, fmt.Errorf("could not determine region code from bucket name: %s", storageBucket)
	}
	log.Printf("Using s3 client for region: %s\n", region.RegionCode)
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region.RegionCode))
	if err != nil {
		return nil, AWSRegion{}, err
	}
	regionalS3Client := s3.NewFromConfig(cfg)

	return regionalS3Client, region, nil
}

// GetRegion from bucket naming scheme format gets the region name from the shortname
func GetRegion(storageBucket string) (AWSRegion, bool) {
	bucketNameTokens := strings.Split(storageBucket, "-")
	shortname := strings.ToLower(bucketNameTokens[len(bucketNameTokens)-1])

	region, exists := AWSRegions[shortname]

	return region, exists
}
