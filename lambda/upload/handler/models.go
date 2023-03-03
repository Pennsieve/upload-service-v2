package handler

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
