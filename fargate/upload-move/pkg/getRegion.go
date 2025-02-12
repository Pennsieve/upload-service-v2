package pkg

import (
	"strings"
)

// GetRegion from bucket naming scheme format gets the region name from the shortname
func GetRegion(storageBucket string) (AWSRegion, bool) {
	bucketNameTokens := strings.Split(storageBucket, "-")
	shortname := strings.ToLower(bucketNameTokens[len(bucketNameTokens)-1])

	region, exists := AWSRegions[shortname]

	return region, exists
}
