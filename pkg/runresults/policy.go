package runresults

// ReadSessionPolicy limits a compute-account role session to reading below
// prefix in bucket, as download-service's run archives do.
func ReadSessionPolicy(bucket, prefix string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":"arn:aws:s3:::` + bucket + "/" + prefix + `*"}]}`
}

// WriteSessionPolicy limits a storage-credentials session to writing one
// manifest's objects (keyPrefix is O{org}/D{ds}/{manifest}): the same grant
// the storage-credentials endpoint gives an uploading client.
func WriteSessionPolicy(bucket, keyPrefix string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject","s3:ListBucketMultipartUploads","s3:AbortMultipartUpload","s3:ListMultipartUploadParts","s3:PutObjectTagging"],"Resource":["arn:aws:s3:::` + bucket + `","arn:aws:s3:::` + bucket + "/" + keyPrefix + `/*"]}]}`
}
