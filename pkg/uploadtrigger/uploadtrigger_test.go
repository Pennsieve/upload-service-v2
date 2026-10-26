package uploadtrigger

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestBody(t *testing.T) {
	var ev events.S3Event
	if err := json.Unmarshal([]byte(Body("storage", "O1/D2/m/u", 5)), &ev); err != nil {
		t.Fatal(err)
	}
	r := ev.Records[0]
	if r.EventName != "ObjectCreated:Put" || r.S3.Bucket.Name != "storage" || r.S3.Object.Key != "O1/D2/m/u" || r.S3.Object.Size != 5 {
		t.Errorf("got %+v", r)
	}
	if v := Attributes("replace")[OnConflictAttrName].StringValue; v == nil || *v != "replace" {
		t.Errorf("attributes: %v", v)
	}
}
