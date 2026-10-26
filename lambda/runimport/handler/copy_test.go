package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/runresults"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/uploadtrigger"
)

type fakeReader struct {
	err  error
	body string
	size int64 // as stored; the body's length when 0
}

func (f fakeReader) Open(context.Context, runresults.Source) (io.ReadCloser, int64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	size := f.size
	if size == 0 {
		size = int64(len(f.body))
	}
	return io.NopCloser(strings.NewReader(f.body)), size, nil
}

type fakeWriter struct {
	err     error
	stored  map[string]string
	tagging string
	// waitForDeadline makes Put take until its context is done.
	waitForDeadline bool
}

func (f *fakeWriter) Put(ctx context.Context, dest runresults.Dest, body io.Reader, _ int64, tagging string) error {
	if f.waitForDeadline {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.err != nil {
		return f.err
	}
	b, _ := io.ReadAll(body)
	if f.stored == nil {
		f.stored = map[string]string{}
	}
	f.stored[dest.Key] = string(b)
	f.tagging = tagging
	return nil
}

type fakeFiles struct {
	status  string
	updated map[string]string // status, message
}

func (f *fakeFiles) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.status == "" {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: map[string]dyTypes.AttributeValue{"Status": &dyTypes.AttributeValueMemberS{Value: f.status}}}, nil
}

func (f *fakeFiles) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.updated = map[string]string{
		"status":  in.ExpressionAttributeValues[":s"].(*dyTypes.AttributeValueMemberS).Value,
		"message": in.ExpressionAttributeValues[":m"].(*dyTypes.AttributeValueMemberS).Value,
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

type fakeQueue struct{ sent []*sqs.SendMessageInput }

func (f *fakeQueue) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.sent = append(f.sent, in)
	return &sqs.SendMessageOutput{}, nil
}

func testJob() runresults.Job {
	return runresults.Job{
		ManifestId: "m", UploadId: "u", RunId: "r", TargetId: "t", OnConflict: "replace", OrgIntId: 7, DatasetIntId: 9,
		Source: runresults.Source{
			Bucket: "workflow-logs-537996532276-dev-n", Key: "imports/r/t/a.json", ReadPrefix: "imports/r/t/",
			RoleArn: "arn:aws:iam::537996532276:role/Pennsieve-Compute-Runtime",
		},
		Dest: runresults.Dest{Bucket: "storage", Key: "O7/D9/m/u", KeyPrefix: "O7/D9/m"},
	}
}

func message(t *testing.T, job runresults.Job, attempt string) events.SQSMessage {
	body, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	return events.SQSMessage{MessageId: "msg-1", Body: string(body), Attributes: map[string]string{"ApproximateReceiveCount": attempt}}
}

func copierWith(r Reader, w Writer, files *fakeFiles, q *fakeQueue) *Copier {
	return &Copier{Reader: r, Writer: w, Files: files, FileTableName: "files", Queue: q, UploadTriggerQueueURL: "upload-queue", MaxAttempts: 3}
}

func TestCopyStoresAndHandsToUpload(t *testing.T) {
	w, files, q := &fakeWriter{}, &fakeFiles{status: "Registered"}, &fakeQueue{}
	resp, _ := copierWith(fakeReader{body: "hello"}, w, files, q).Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{message(t, testJob(), "1")}})
	if len(resp.BatchItemFailures) != 0 {
		t.Fatalf("retry asked: %+v", resp)
	}
	if w.stored["O7/D9/m/u"] != "hello" || w.tagging != "DatasetId=9&OrgId=7" {
		t.Errorf("stored %v, tagging %q", w.stored, w.tagging)
	}
	if len(q.sent) != 1 {
		t.Fatalf("handed to upload %d times", len(q.sent))
	}
	var ev events.S3Event
	_ = json.Unmarshal([]byte(*q.sent[0].MessageBody), &ev)
	if ev.Records[0].S3.Object.Key != "O7/D9/m/u" || ev.Records[0].S3.Bucket.Name != "storage" {
		t.Errorf("event: %+v", ev)
	}
	if ev.Records[0].S3.Object.Size != 5 {
		t.Errorf("size %d, want the stored file's", ev.Records[0].S3.Object.Size)
	}
	if v := q.sent[0].MessageAttributes[uploadtrigger.OnConflictAttrName].StringValue; v == nil || *v != "replace" {
		t.Errorf("onConflict not passed: %v", v)
	}
}

func TestCopySkipsDoneFiles(t *testing.T) {
	for _, s := range []string{"Finalized", "Verified", "Failed", ""} {
		w, q := &fakeWriter{}, &fakeQueue{}
		resp, _ := copierWith(fakeReader{body: "x"}, w, &fakeFiles{status: s}, q).Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{message(t, testJob(), "1")}})
		if len(resp.BatchItemFailures) != 0 || len(w.stored) != 0 || len(q.sent) != 0 {
			t.Errorf("status %q: copied or retried", s)
		}
	}
}

func TestCopyFailures(t *testing.T) {
	cases := []struct {
		name      string
		reader    Reader
		writer    *fakeWriter
		job       func(j runresults.Job) runresults.Job
		attempt   string
		wantRetry bool
		wantMsg   string
	}{
		{"a staged file that's gone", fakeReader{err: &s3Types.NoSuchKey{}}, &fakeWriter{}, nil, "1", false, "the staged file is gone"},
		// The stored size, whatever the request said.
		{"a file too large", fakeReader{body: "x", size: runresults.MaxFileBytes + 1}, &fakeWriter{}, nil, "1", false, "larger than"},
		{"a storage error, retried", fakeReader{body: "x"}, &fakeWriter{err: errors.New("slow down")}, nil, "1", true, ""},
		{"a storage error, last attempt", fakeReader{body: "x"}, &fakeWriter{err: errors.New("AccessDenied: arn:aws:iam::941165240011:role/secret")}, nil, "3", false, "could not be stored"},
	}
	for _, c := range cases {
		job := testJob()
		if c.job != nil {
			job = c.job(job)
		}
		files := &fakeFiles{status: "Registered"}
		resp, _ := copierWith(c.reader, c.writer, files, &fakeQueue{}).Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{message(t, job, c.attempt)}})
		if retried := len(resp.BatchItemFailures) == 1; retried != c.wantRetry {
			t.Errorf("%s: retry %v, want %v", c.name, retried, c.wantRetry)
		}
		if c.wantMsg == "" {
			if files.updated != nil {
				t.Errorf("%s: marked %v", c.name, files.updated)
			}
			continue
		}
		if files.updated["status"] != "Failed" || !strings.Contains(files.updated["message"], c.wantMsg) {
			t.Errorf("%s: marked %v", c.name, files.updated)
		}
		// Dataset viewers see the message: no account ids or ARNs.
		if strings.Contains(files.updated["message"], "arn:") || strings.Contains(files.updated["message"], "941165240011") {
			t.Errorf("%s: the message leaks %q", c.name, files.updated["message"])
		}
	}
}

func TestCopyDropsUnexpectedJobs(t *testing.T) {
	for name, change := range map[string]func(j *runresults.Job){
		"another prefix":     func(j *runresults.Job) { j.Source.ReadPrefix = "results/r/"; j.Source.Key = "results/r/a" },
		"a key outside it":   func(j *runresults.Job) { j.Source.Key = "imports/r/other/a" },
		"another bucket":     func(j *runresults.Job) { j.Source.Bucket = "pennsieve-dev-storage" },
		"another role":       func(j *runresults.Job) { j.Source.RoleArn = "arn:aws:iam::941165240011:role/admin" },
		"another dest key":   func(j *runresults.Job) { j.Dest.Key = "O7/D9/other/u" },
		"another dest place": func(j *runresults.Job) { j.Dest.KeyPrefix = "O1/D1/m"; j.Dest.Key = "O1/D1/m/u" },
	} {
		job := testJob()
		change(&job)
		w, files, q := &fakeWriter{}, &fakeFiles{status: "Registered"}, &fakeQueue{}
		resp, _ := copierWith(fakeReader{body: "x"}, w, files, q).Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{message(t, job, "1")}})
		if len(resp.BatchItemFailures) != 0 || len(w.stored) != 0 || len(q.sent) != 0 || files.updated != nil {
			t.Errorf("%s: acted on", name)
		}
	}
}

// A copy that would outlast the lambda gives up in time to mark the file.
func TestCopyGivesUpBeforeTheLambdaDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(copyMargin+50*time.Millisecond))
	defer cancel()
	files := &fakeFiles{status: "Registered"}
	resp, _ := copierWith(fakeReader{body: "x"}, &fakeWriter{waitForDeadline: true}, files, &fakeQueue{}).Handle(ctx, events.SQSEvent{Records: []events.SQSMessage{message(t, testJob(), "1")}})
	if len(resp.BatchItemFailures) != 0 || files.updated["status"] != "Failed" || !strings.Contains(files.updated["message"], "in time") {
		t.Errorf("retry %v, marked %v", resp.BatchItemFailures, files.updated)
	}
}

func TestPartSize(t *testing.T) {
	if partSize(1<<20) != 16<<20 {
		t.Error("small files: 16 MiB parts")
	}
	big := int64(runresults.MaxFileBytes)
	if n := (big + partSize(big) - 1) / partSize(big); n > 10000 {
		t.Errorf("%d parts for the largest file", n)
	}
}
