// External test package — avoids the in-package TestMain that requires live
// DynamoDB/Postgres for the integration tests.
package handler_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestKeyRegex confirms the regex used in uploadEntryFromS3Event accepts both
// the legacy upload-bucket key shape, the direct-to-storage shape, and nested
// sub-paths permitted by the upload-credentials session policy. Kept in sync
// with the literal in handler/s3.go by copy; integration tests (s3_test.go)
// exercise the live path.
func TestKeyRegex(t *testing.T) {
	r := regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<UploadId>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})`)

	manifestId := "00000000-0000-0000-0000-000000000000"
	uploadId := "00000000-1111-1111-1111-000000000000"

	cases := []struct {
		name  string
		key   string
		match bool
	}{
		{"legacy upload bucket", manifestId + "/" + uploadId, true},
		{"direct to storage", "O42/D17/" + manifestId + "/" + uploadId, true},
		{"direct to storage large ids", "O1234567/D7654321/" + manifestId + "/" + uploadId, true},
		{"nested subpath (data-target compat)", manifestId + "/" + uploadId + "/extracted/file.csv", true},
		{"malformed no manifest", "not-a-uuid/not-a-uuid", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := r.FindStringSubmatch(tc.key)
			if !tc.match {
				assert.Nil(t, res, "unexpected match for %q", tc.key)
				return
			}
			if !assert.NotNil(t, res, "expected match for %q", tc.key) {
				return
			}
			assert.Equal(t, manifestId, res[r.SubexpIndex("Manifest")])
			assert.Equal(t, uploadId, res[r.SubexpIndex("UploadId")])
		})
	}
}
