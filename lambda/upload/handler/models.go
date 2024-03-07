package handler

import "fmt"

type S3FileNotExistError struct {
	ManifestId string
	UploadId   string
	S3Bucket   string
	S3Key      string
}

func (e *S3FileNotExistError) Error() string {
	return fmt.Sprintf("S3 File cannot be read: %s / %s ", e.S3Bucket, e.S3Key)
}

type S3FileMalFormedError struct {
	S3Bucket string
	S3Key    string
}

func (e *S3FileMalFormedError) Error() string {
	return fmt.Sprintf("S3 File is not structured as expected: %s / %s ", e.S3Bucket, e.S3Key)
}

// UploadEntry representation of file from SQS queue on Upload Trigger
type UploadEntry struct {
	ManifestId     string
	UploadId       string
	S3Bucket       string
	S3Key          string
	Path           string
	Name           string
	Extension      string
	ETag           string
	Size           int64
	MergePackageId string
	FileType       string
	Sha256         string
}
