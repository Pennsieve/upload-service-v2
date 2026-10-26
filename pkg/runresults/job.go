package runresults

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// MaxFileBytes is the largest file the run-import lambda copies: a copy
// streams through the lambda, which has 15 minutes (less a margin) at a few
// tens of MiB/s. A bigger file fails with a clear error rather than timing
// out.
const MaxFileBytes = 40 << 30

// Job is one file to copy, queued by the service lambda for the run-import
// lambda. The source is a run's staged file, read with the compute
// account's role (limited to the target's prefix); the destination is the
// manifest's storage key, written with the storage credentials role.
type Job struct {
	ManifestId string `json:"manifestId"`
	UploadId   string `json:"uploadId"`
	RunId      string `json:"runId"`
	TargetId   string `json:"targetId"`
	OnConflict string `json:"onConflict"`
	// OrgIntId and DatasetIntId tag the stored object, as uploads are.
	OrgIntId     int64 `json:"orgIntId"`
	DatasetIntId int64 `json:"datasetIntId"`

	Source Source `json:"source"`
	Dest   Dest   `json:"dest"`
}

// Source is a staged file in a compute account.
type Source struct {
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	ReadPrefix string `json:"readPrefix"`
	RoleArn    string `json:"roleArn"`
	ExternalId string `json:"externalId,omitempty"`
	Region     string `json:"region"`
}

// Dest is where the file goes in Pennsieve storage.
type Dest struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	KeyPrefix string `json:"keyPrefix"` // the manifest's O{org}/D{ds}/{manifest}
	Region    string `json:"region"`
}

// Check reports a job that isn't what the service lambda queues: its source
// below its run target's prefix, its destination its manifest's upload key,
// reading with a compute-account role. The run-import lambda checks every
// job it takes, so a message it didn't expect can't widen what it reads or
// writes.
func (j Job) Check() error {
	switch {
	case j.ManifestId == "" || j.UploadId == "" || j.RunId == "" || j.TargetId == "":
		return errors.New("job is missing its manifest, upload, run or target")
	case j.Source.ReadPrefix != Prefix(j.RunId, j.TargetId):
		return fmt.Errorf("job reads %q, not its run target's prefix", j.Source.ReadPrefix)
	case !strings.HasPrefix(j.Source.Key, j.Source.ReadPrefix) || strings.Contains(j.Source.Key, ".."):
		return fmt.Errorf("job's file %q isn't below its prefix", j.Source.Key)
	case !strings.HasPrefix(j.Source.Bucket, "workflow-logs-"):
		return fmt.Errorf("job reads bucket %q, not a compute node's", j.Source.Bucket)
	case !computeRoleRE.MatchString(j.Source.RoleArn):
		return fmt.Errorf("job reads with %q, not a compute account role", j.Source.RoleArn)
	case j.Dest.KeyPrefix != fmt.Sprintf("O%d/D%d/%s", j.OrgIntId, j.DatasetIntId, j.ManifestId) || j.Dest.Key != j.Dest.KeyPrefix+"/"+j.UploadId:
		return fmt.Errorf("job writes %q, not its manifest's upload key", j.Dest.Key)
	}
	return nil
}

// FilePlacement is where a staged file goes in the dataset: its folder (the
// target's folder plus the file's own folders) and name. key must be below
// prefix; neither it nor the folder may have empty, "." or ".." segments.
func FilePlacement(prefix, key, targetFolder string) (folder, name string, err error) {
	rel, ok := strings.CutPrefix(key, prefix)
	if !ok || rel == "" || strings.HasSuffix(rel, "/") {
		return "", "", fmt.Errorf("%q is not a file below %q", key, prefix)
	}
	if err := cleanSegments(rel); err != nil {
		return "", "", fmt.Errorf("%q: %w", key, err)
	}
	targetFolder = strings.Trim(targetFolder, "/")
	if targetFolder != "" {
		if err := cleanSegments(targetFolder); err != nil {
			return "", "", fmt.Errorf("target folder %q: %w", targetFolder, err)
		}
	}
	dir, name := path.Split(rel)
	folder = strings.Trim(path.Join(targetFolder, dir), "/")
	if folder == "." {
		folder = ""
	}
	return folder, name, nil
}

func cleanSegments(p string) error {
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return errors.New("has an empty, . or .. segment")
		}
	}
	return nil
}

// ErrTooLarge is a file over MaxFileBytes.
var ErrTooLarge = errors.New("file is larger than run imports copy")
