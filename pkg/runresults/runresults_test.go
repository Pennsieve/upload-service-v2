package runresults

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

func TestToken(t *testing.T) {
	service, run, token, ok := Token("Callback workflow-service:run-1:abc:def")
	if !ok || service != "workflow-service" || run != "run-1" || token != "abc:def" {
		t.Fatalf("got %q %q %q %v", service, run, token, ok)
	}
	for _, bad := range []string{"Bearer x", "Callback a:b", "Callback :b:c", ""} {
		if _, _, _, ok := Token(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
}

type fakeLambda struct{ resp validateResponse }

func (f fakeLambda) Invoke(_ context.Context, in *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	body, _ := json.Marshal(f.resp)
	return &lambda.InvokeOutput{Payload: body}, nil
}

func TestValidate(t *testing.T) {
	v := &Validator{Lambda: fakeLambda{validateResponse{IsAuthorized: true, UserNodeId: "N:user:1", ComputeNodeId: "node-1"}}, FunctionName: "f"}
	run, err := v.Validate(context.Background(), "run-1", "t")
	if err != nil || run.ComputeNodeId != "node-1" || run.UserNodeId != "N:user:1" {
		t.Fatalf("got %+v %v", run, err)
	}
	v.Lambda = fakeLambda{validateResponse{IsAuthorized: false, Error: "run is over"}}
	if _, err := v.Validate(context.Background(), "run-1", "t"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}

type fakeDB map[string]map[string]types.AttributeValue

func (f fakeDB) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	uuid := in.Key["uuid"].(*types.AttributeValueMemberS).Value
	return &dynamodb.GetItemOutput{Item: f[aws.ToString(in.TableName)+"/"+uuid]}, nil
}

func item(t *testing.T, v any) map[string]types.AttributeValue {
	m, err := attributevalue.MarshalMap(v)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestLocate(t *testing.T) {
	db := fakeDB{
		"nodes/node-1":    item(t, map[string]string{"accountUuid": "acct-1", "environment": "dev", "identifier": "ac409ea1"}),
		"accounts/acct-1": item(t, map[string]string{"accountId": "537996532276", "roleName": "ROLE-old", "runtimeRoleName": "Pennsieve-Compute-Runtime", "externalId": "ext"}),
	}
	l := &Locator{DB: db, NodesTable: "nodes", AccountsTable: "accounts", Region: "us-east-1"}
	res, err := l.Locate(context.Background(), Run{Id: "run-1", ComputeNodeId: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Bucket != "workflow-logs-537996532276-dev-ac409ea1" || res.RoleArn != "arn:aws:iam::537996532276:role/Pennsieve-Compute-Runtime" || res.ExternalId != "ext" {
		t.Fatalf("got %+v", res)
	}
	if _, err := l.Locate(context.Background(), Run{ComputeNodeId: "missing"}); err == nil {
		t.Fatal("a missing node located")
	}
}

func TestFilePlacement(t *testing.T) {
	prefix := Prefix("run-1", "dataset-llm")
	cases := []struct{ key, folder, wantFolder, wantName string }{
		{prefix + "llm-result.json", "e2e-results", "e2e-results", "llm-result.json"},
		{prefix + "copies/signals/a.csv", "e2e-results", "e2e-results/copies/signals", "a.csv"},
		{prefix + "a.csv", "", "", "a.csv"},
		{prefix + "sub/a.csv", "/out/", "out/sub", "a.csv"},
	}
	for _, c := range cases {
		folder, name, err := FilePlacement(prefix, c.key, c.folder)
		if err != nil || folder != c.wantFolder || name != c.wantName {
			t.Errorf("%s: got %q %q %v", c.key, folder, name, err)
		}
	}
	for _, bad := range []string{"imports/run-2/dataset-llm/a.csv", prefix + "../x", prefix + "a//b", prefix, prefix + "dir/"} {
		if _, _, err := FilePlacement(prefix, bad, ""); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	// The target's folder can't climb out of the dataset either.
	for _, folder := range []string{"../../x", "a/../../b", "./a"} {
		if _, _, err := FilePlacement(prefix, prefix+"a.csv", folder); err == nil {
			t.Errorf("folder %q accepted", folder)
		}
	}
}

func TestPolicies(t *testing.T) {
	for _, p := range []string{ReadSessionPolicy("b", "results/r/t/"), WriteSessionPolicy("b", "O1/D2/m")} {
		if !json.Valid([]byte(p)) {
			t.Errorf("invalid JSON: %s", p)
		}
	}
	if !strings.Contains(ReadSessionPolicy("b", "imports/r/t/"), "arn:aws:s3:::b/imports/r/t/*") {
		t.Error("read policy isn't limited to the prefix")
	}
	if !strings.Contains(WriteSessionPolicy("b", "O1/D2/m"), "arn:aws:s3:::b/O1/D2/m/*") {
		t.Error("write policy isn't limited to the manifest")
	}
}

func validJob() Job {
	return Job{
		ManifestId: "m", UploadId: "u", RunId: "r", TargetId: "t", OrgIntId: 7, DatasetIntId: 9,
		Source: Source{Bucket: "workflow-logs-1-dev-n", Key: "imports/r/t/a.json", ReadPrefix: "imports/r/t/", RoleArn: "arn:aws:iam::537996532276:role/ROLE-abc"},
		Dest:   Dest{Bucket: "storage", Key: "O7/D9/m/u", KeyPrefix: "O7/D9/m"},
	}
}

func TestJobCheck(t *testing.T) {
	if err := validJob().Check(); err != nil {
		t.Fatalf("a valid job: %v", err)
	}
	for name, change := range map[string]func(j *Job){
		"no target":         func(j *Job) { j.TargetId = "" },
		"another prefix":    func(j *Job) { j.Source.ReadPrefix = "imports/r/" },
		"a key outside":     func(j *Job) { j.Source.Key = "imports/r/u/a" },
		"a key escaping":    func(j *Job) { j.Source.Key = "imports/r/t/../u/a" },
		"another bucket":    func(j *Job) { j.Source.Bucket = "storage" },
		"another role":      func(j *Job) { j.Source.RoleArn = "arn:aws:iam::537996532276:role/admin" },
		"another dataset":   func(j *Job) { j.DatasetIntId = 1 },
		"another upload id": func(j *Job) { j.Dest.Key = "O7/D9/m/x" },
	} {
		j := validJob()
		change(&j)
		if err := j.Check(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestImportTarget(t *testing.T) {
	run := Run{DatasetTargets: []DatasetTarget{
		{NodeId: "a", TargetType: TargetTypePennsieveDataset, DatasetNodeId: "N:dataset:1", TargetFolder: "out"},
		{NodeId: "other", TargetType: "Some Container", DatasetNodeId: "N:dataset:1"},
	}}
	if got, err := run.ImportTarget("a", "N:dataset:1"); err != nil || got.TargetFolder != "out" {
		t.Fatalf("got %+v %v", got, err)
	}
	for _, c := range [][2]string{{"a", "N:dataset:2"}, {"other", "N:dataset:1"}, {"missing", "N:dataset:1"}} {
		if _, err := run.ImportTarget(c[0], c[1]); !errors.Is(err, ErrNotATarget) {
			t.Errorf("%v accepted", c)
		}
	}
}
