package pkg

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// maxPartSize constant for number of bits in 50 megabyte chunk
// this corresponds with max file size of 500GB per file as copy can do max 10,000 parts.
const maxPartSize = 105 * 1024 * 1024

// nrCopyWorkers number of threads for multipart uploader
const nrCopyWorkers = 10

// MultiPartCopy function that starts, perform each part upload, and completes the copy
func MultiPartCopy(svc *s3.Client, timeout time.Duration, fileSize int64, sourceBucket string, sourceKey string, destBucket string, destKey string) error {

	partWalker := make(chan s3.UploadPartCopyInput, nrCopyWorkers)
	results := make(chan s3types.CompletedPart, nrCopyWorkers)

	parts := make([]s3types.CompletedPart, 0)

	ctx, cancelFn := context.WithTimeout(context.TODO(), timeout)
	defer cancelFn()

	//struct for starting a multipart upload
	startInput := s3.CreateMultipartUploadInput{
		Bucket: &destBucket,
		Key:    &destKey,
	}

	//send command to start copy and get the upload id as it is needed later
	var uploadId string
	createOutput, err := svc.CreateMultipartUpload(ctx, &startInput)
	if err != nil {
		return err
	}
	if createOutput != nil {
		if createOutput.UploadId != nil {
			uploadId = *createOutput.UploadId
		}
	}
	if uploadId == "" {
		return errors.New("no upload id found in start upload request")
	}

	//numUploads := fileSize / maxPartSize
	//log.Printf("Will attempt upload in %d number of parts to %s\n", numUploads, destKey)

	// Walk over all files in IMPORTED status and make available on channel for processors.
	go allocate(uploadId, fileSize, sourceBucket, sourceKey, destBucket, destKey, partWalker)

	done := make(chan bool)

	go aggregateResult(done, &parts, results)

	// Wait until all processors are completed.
	createWorkerPool(ctx, svc, nrCopyWorkers, uploadId, partWalker, results)

	// Wait until done channel has a value
	<-done

	// sort parts (required for complete method
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	//create struct for completing the upload
	mpu := s3types.CompletedMultipartUpload{
		Parts: parts,
	}

	//complete actual upload
	complete := s3.CompleteMultipartUploadInput{
		Bucket:          &destBucket,
		Key:             &destKey,
		UploadId:        &uploadId,
		MultipartUpload: &mpu,
	}
	compOutput, err := svc.CompleteMultipartUpload(context.TODO(), &complete)
	if err != nil {
		return fmt.Errorf("error completing upload: %w", err)
	}
	if compOutput != nil {
		log.Printf("Successfully copied: %s Key: %s to Bucket: %s Key: %s\n", sourceBucket, sourceKey, destBucket, destKey)
	}
	return nil
}

// buildCopySourceRange helper function to build the string for the range of bits to copy
func buildCopySourceRange(start int64, objectSize int64) string {
	end := start + maxPartSize - 1
	if end > objectSize {
		end = objectSize - 1
	}
	startRange := strconv.FormatInt(start, 10)
	stopRange := strconv.FormatInt(end, 10)
	return "bytes=" + startRange + "-" + stopRange
}

// allocate create entries into the chunk channel for the workers to consume.
func allocate(uploadId string, fileSize int64, sourceBucket string, sourceKey string, destBucket string, destKey string, partWalker chan s3.UploadPartCopyInput) {
	defer func() {
		close(partWalker)
	}()

	var i int64
	var partNumber int32 = 1
	for i = 0; i < fileSize; i += maxPartSize {
		copySourceRange := buildCopySourceRange(i, fileSize)
		copySource := "/" + sourceBucket + "/" + sourceKey
		partWalker <- s3.UploadPartCopyInput{
			Bucket:          &destBucket,
			CopySource:      &copySource,
			CopySourceRange: &copySourceRange,
			Key:             &destKey,
			PartNumber:      partNumber,
			UploadId:        &uploadId,
		}
		partNumber++
	}
}

// createWorkerPool creates a worker pool for uploading parts
func createWorkerPool(ctx context.Context, svc *s3.Client, nrWorkers int, uploadId string,
	partWalker chan s3.UploadPartCopyInput, results chan s3types.CompletedPart) {

	defer func() {
		close(results)
	}()

	var copyWg sync.WaitGroup
	workerFailed := false
	for w := 1; w <= nrWorkers; w++ {
		copyWg.Add(1)
		log.Println("starting upload-part worker:", w)
		w := int32(w)
		go func() {
			err := worker(ctx, svc, &copyWg, w, partWalker, results)
			if err != nil {
				workerFailed = true
			}
		}()

	}

	// Wait until all workers are finished
	copyWg.Wait()

	// Check if workers finished due to error
	if workerFailed {
		log.Println("Attempting to abort upload")
		abortIn := s3.AbortMultipartUploadInput{
			UploadId: aws.String(uploadId),
		}
		//ignoring any errors with aborting the copy
		_, err := svc.AbortMultipartUpload(context.TODO(), &abortIn)
		if err != nil {
			log.Println("Error aborting failed upload session.")
		}
	}

	log.Println("Finished checking status of workers.")
}

// aggregateResult grabs the e-tags from results channel and aggregates in array
func aggregateResult(done chan bool, parts *[]s3types.CompletedPart, results chan s3types.CompletedPart) {

	for cPart := range results {
		*parts = append(*parts, cPart)
	}

	done <- true
}

// worker uploads parts of a file as part of copy function.
func worker(ctx context.Context, svc *s3.Client, wg *sync.WaitGroup, workerId int32,
	partWalker chan s3.UploadPartCopyInput, results chan s3types.CompletedPart) error {

	// Close worker after it completes.
	// This happens when the items channel closes.
	defer func() {
		log.Println("Closing UploadPart Worker: ", workerId)
		wg.Done()
	}()

	for partInput := range partWalker {

		//log.Printf("Attempting to upload part %d range: %s\n", partInput.PartNumber, *partInput.CopySourceRange)
		partResp, err := svc.UploadPartCopy(ctx, &partInput)

		if err != nil {
			return err
		}

		//copy etag and part number from response as it is needed for completion
		if partResp != nil {
			partNum := partInput.PartNumber
			etag := strings.Trim(*partResp.CopyPartResult.ETag, "\"")
			cPart := s3types.CompletedPart{
				ETag:       &etag,
				PartNumber: partNum,
			}

			results <- cPart

			log.Printf("Successfully upload part %d of %s\n", partInput.PartNumber, *partInput.UploadId)
		}

	}

	return nil

}

// CreateDefaultClient creates a client for the appropriate region data is being copied to
func CreateDefaultClient(storageBucket string) (*s3.Client, string, error) {

	// Get region
	region := GetRegion(storageBucket)
	if region.RegionCode == "" {
		return nil, "", errors.New("could not determine region code")
	}
	log.Printf("Using s3 client for region: %s\n", region.RegionCode)
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region.RegionCode))
	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}
	regionalS3Client := s3.NewFromConfig(cfg)

	return regionalS3Client, region.RegionCode, nil
}

// GetRegion from bucket naming scheme format gets the region name from the shortname
func GetRegion(storageBucket string) AWSRegions {
	bucketNameTokens := strings.Split(storageBucket, "-")
	shortname := strings.ToLower(bucketNameTokens[len(bucketNameTokens)-1])

	region := Regions[shortname]

	return region
}
