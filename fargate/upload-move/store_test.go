package main

import (
	"github.com/pennsieve/pennsieve-upload-service-v2/upload-move-files/pkg"
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
