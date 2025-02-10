package pkg

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"strings"
)

// CreateClient creates a client for the appropriate region data is being copied to
func CreateClient(storageBucket string) (*s3.Client, string, error) {

	// Get region
	region := GetRegion(storageBucket)
	if region.RegionCode == "" {
		return nil, "", errors.New("could not determine region code")
	}
	log.Printf("Using s3 client for region: %s\n", region.RegionCode)
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region.RegionCode))
	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}
	regionalS3Client := s3.NewFromConfig(cfg)

	return regionalS3Client, region.RegionCode, nil
}

// GetRegion from bucket naming scheme format gets the region name from the shortname
func GetRegion(storageBucket string) AWSRegions {
	bucketNameTokens := strings.Split(storageBucket, "-")
	shortname := strings.ToLower(bucketNameTokens[len(bucketNameTokens)-1])

	region := Regions[shortname]

	return region
}
