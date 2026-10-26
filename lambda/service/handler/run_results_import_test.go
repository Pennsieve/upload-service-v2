package handler

import (
	"strings"
	"testing"

	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/runresults"
	"github.com/pennsieve/pennsieve-upload-service-v2/service/pkg/storage"
)

const testRun = "2cc3b55f-185a-48f9-b9f9-fb4b4ba40815"

func importRequest(keys ...string) runResultsImportRequest {
	req := runResultsImportRequest{RunId: testRun, TargetId: "dataset-llm", TargetFolder: "e2e-results"}
	for _, k := range keys {
		req.Objects = append(req.Objects, runResultsObject{Key: k, Size: 10})
	}
	return req
}

func TestCheckRunImportRequest(t *testing.T) {
	auth := "Callback workflow-service:" + testRun + ":secret"
	req := importRequest(runresults.Prefix(testRun, "dataset-llm") + "a.json")
	token, err := checkRunImportRequest(req, auth)
	if err != nil || token != "secret" {
		t.Fatalf("got %q %v", token, err)
	}
	bad := map[string]func(r *runResultsImportRequest) string{
		"a user token":    func(r *runResultsImportRequest) string { return "Bearer x" },
		"another service": func(r *runResultsImportRequest) string { return "Callback other:" + testRun + ":t" },
		"another run":     func(r *runResultsImportRequest) string { r.RunId = "other"; return auth },
		"a bad target id": func(r *runResultsImportRequest) string { r.TargetId = "../x"; return auth },
		"no files":        func(r *runResultsImportRequest) string { r.Objects = nil; return auth },
		"too many files": func(r *runResultsImportRequest) string {
			r.Objects = make([]runResultsObject, maxRunImportObjects+1)
			return auth
		},
		"an unknown conflict": func(r *runResultsImportRequest) string { r.OnConflict = "merge"; return auth },
	}
	for name, change := range bad {
		r := req
		a := change(&r)
		if _, err := checkRunImportRequest(r, a); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestPlanRunImport(t *testing.T) {
	prefix := runresults.Prefix(testRun, "dataset-llm")
	req := importRequest(prefix+"llm-result.json", prefix+"copies/signals/a.csv")
	manifestId := runImportManifestId(testRun, "dataset-llm", "N:dataset:1")
	src := runresults.Results{Bucket: "workflow-logs-1-dev-n", RoleArn: "arn:aws:iam::1:role/R", ExternalId: "ext", Region: "us-east-1"}
	dest := storage.Resolution{OrganizationId: 7, DatasetId: 9, StorageBucket: "pennsieve-dev-storage-use1"}

	files, jobs, err := planRunImport(req, manifestId, src, dest, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || len(jobs) != 2 {
		t.Fatalf("got %d files, %d jobs", len(files), len(jobs))
	}
	if files[1].TargetPath != "e2e-results/copies/signals" || files[1].TargetName != "a.csv" {
		t.Errorf("placement: %+v", files[1])
	}
	wantKey := "O7/D9/" + manifestId + "/" + files[0].UploadID
	if files[0].S3Key != wantKey || jobs[0].Dest.Key != wantKey || jobs[0].Dest.KeyPrefix != "O7/D9/"+manifestId {
		t.Errorf("destination: %q, %+v", files[0].S3Key, jobs[0].Dest)
	}
	j := jobs[0]
	if j.Source.ReadPrefix != prefix || j.Source.RoleArn != src.RoleArn || j.Source.ExternalId != "ext" || j.OrgIntId != 7 || j.DatasetIntId != 9 {
		t.Errorf("job: %+v", j)
	}
	// The same page again plans the same rows: retries are idempotent.
	again, _, _ := planRunImport(req, manifestId, src, dest, "us-east-1")
	if again[0].UploadID != files[0].UploadID {
		t.Error("upload ids differ between attempts")
	}

	for name, key := range map[string]string{
		"another run's file": "results/other/dataset-llm/a.json",
		"another target's":   runresults.Prefix(testRun, "download") + "a.json",
		"a path escape":      prefix + "../../x",
	} {
		if _, _, err := planRunImport(importRequest(key), manifestId, src, dest, "us-east-1"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, _, err := planRunImport(importRequest(prefix+"a", prefix+"a"), manifestId, src, dest, "us-east-1"); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Errorf("a duplicate key: %v", err)
	}
}

func TestRunImportIds(t *testing.T) {
	a := runImportManifestId(testRun, "t", "N:dataset:1")
	if a != runImportManifestId(testRun, "t", "N:dataset:1") || !isValidUUID(a) {
		t.Fatal("manifest id isn't a stable UUID")
	}
	if a == runImportManifestId(testRun, "t", "N:dataset:2") || a == runImportManifestId(testRun, "u", "N:dataset:1") {
		t.Fatal("different targets or datasets share a manifest")
	}
}

func TestCountRunImport(t *testing.T) {
	s := countRunImport("m", []runImportFileRow{
		{Status: "Finalized"}, {Status: "Verified"},
		{Status: "Registered"}, {Status: "Imported"},
		{Status: "Failed", FilePath: "e2e-results", FileName: "big.bin", Message: "file is larger than run imports copy"},
		{Status: "FailedOrphan", FileName: "gone.csv"},
	})
	if s.Total != 6 || s.Imported != 2 || s.InProgress != 2 || s.Failed != 2 {
		t.Fatalf("got %+v", s)
	}
	if s.Failures[0].Path != "e2e-results/big.bin" || s.Failures[1].Path != "gone.csv" {
		t.Errorf("failures: %+v", s.Failures)
	}
}
