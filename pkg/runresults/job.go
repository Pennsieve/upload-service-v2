package runresults

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// MaxFileBytes is the largest file the run-import lambda copies: a copy
// streams through the lambda, which has 15 minutes. A bigger file fails
// with a clear error rather than timing out.
const MaxFileBytes = 100 << 30

// Job is one file to copy, queued by the service lambda for the run-import
// lambda. The source is a run's staged result, read with the compute
// account's role (limited to the target's prefix); the destination is the
// manifest's storage key, written with the storage credentials role.
type Job struct {
	ManifestId string `json:"manifestId"`
	UploadId   string `json:"uploadId"`
	RunId      string `json:"runId"`
	OnConflict string `json:"onConflict"`
	// OrgIntId and DatasetIntId tag the stored object, as uploads are.
	OrgIntId     int64 `json:"orgIntId"`
	DatasetIntId int64 `json:"datasetIntId"`

	Source Source `json:"source"`
	Dest   Dest   `json:"dest"`
	Size   int64  `json:"size"`
}

// Source is a staged result in a compute account.
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

// FilePlacement is where a staged result goes in the dataset: its folder
// (the target's folder plus the result's own folders) and name. key must be
// below prefix, with no "." or ".." segments.
func FilePlacement(prefix, key, targetFolder string) (folder, name string, err error) {
	rel, ok := strings.CutPrefix(key, prefix)
	if !ok || rel == "" || strings.HasSuffix(rel, "/") {
		return "", "", fmt.Errorf("%q is not a file below %q", key, prefix)
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", "", fmt.Errorf("%q has an empty, . or .. segment", key)
		}
	}
	dir, name := path.Split(rel)
	folder = strings.Trim(path.Join(strings.Trim(targetFolder, "/"), dir), "/")
	if folder == "." {
		folder = ""
	}
	return folder, name, nil
}

// ErrTooLarge is a file over MaxFileBytes.
var ErrTooLarge = errors.New("file is larger than run imports copy")
