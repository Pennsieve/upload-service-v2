// External test package — avoids the in-package TestMain that requires live
// DynamoDB/Postgres for the integration tests. These tests only verify pure
// wire-format + pure-function behavior and should be runnable in any
// environment.
package handler_test

import (
	"encoding/json"
	"testing"

	"github.com/pennsieve/pennsieve-upload-service-v2/upload/handler"
	"github.com/stretchr/testify/assert"
)

// TestDeletePackageJobEnvelope pins the Scala-compatible wire format for
// DeletePackageJob messages published to the platform jobs_queue. Scala's
// Circe uses wrapped-discriminator serialization for the BackgroundJob
// sealed trait: the class name is the outer JSON key and the fields live
// in a nested object. TraceId is an unwrapped value class → bare string.
// If this test fails, the Scala jobs service will reject our messages
// silently (they go to the DLQ) and S3 asset cleanup won't fire after a
// replace. See pennsieve-api/core/src/main/scala/com/pennsieve/messages/BackgroundJob.scala.
// The envelope shape is the body of each SendMessageBatch entry — batching
// at the SQS transport layer doesn't change the per-message JSON.
func TestDeletePackageJobEnvelope(t *testing.T) {
	payload := handler.DeletePackageJobEnvelope{
		DeletePackageJob: handler.DeletePackageJobInner{
			PackageId:      123,
			OrganizationId: 1,
			UserId:         "N:user:abc",
			TraceId:        "manifest-42",
			Id:             "b7d5f0c2-1111-2222-3333-444444444444",
		},
	}
	b, err := json.Marshal(payload)
	assert.NoError(t, err)

	var decoded map[string]map[string]interface{}
	assert.NoError(t, json.Unmarshal(b, &decoded))

	outer, ok := decoded["DeletePackageJob"]
	assert.True(t, ok, "outer key must be DeletePackageJob (wrapped discriminator)")
	assert.Equal(t, float64(123), outer["packageId"])
	assert.Equal(t, float64(1), outer["organizationId"])
	assert.Equal(t, "N:user:abc", outer["userId"])
	assert.Equal(t, "manifest-42", outer["traceId"],
		"traceId must be a bare string (Circe deriveUnwrappedEncoder), not an object")
	assert.Equal(t, "b7d5f0c2-1111-2222-3333-444444444444", outer["id"])

	// No other top-level keys — the envelope is single-key.
	assert.Len(t, decoded, 1)
}