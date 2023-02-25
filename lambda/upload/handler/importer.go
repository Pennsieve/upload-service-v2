package handler

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/pgdb/models"
	log "github.com/sirupsen/logrus"
	"regexp"
)

// uploadEntry representation of file from SQS queue on Upload Trigger
type uploadEntry struct {
	manifestId     string
	uploadId       string
	s3Bucket       string
	s3Key          string
	path           string
	name           string
	extension      string
	eTag           string
	size           int64
	mergePackageId string
	fileType       string
	sha256         string
}

//// UploadSession contains the information that is shared based on the manifest ID
//type UploadSession struct {
//	organizationId  int
//	datasetId       int
//	datasetNodeId   string
//	ownerId         int
//	targetPackageId string
//	db              *sql.DB
//}

// PublishToSNS publishes messages to SNS after files are imported.
//func (s *UploadSession) PublishToSNS(files []models.File) error {
//
//	const batchSize = 10
//	var snsEntries []types.PublishBatchRequestEntry
//	for _, f := range files {
//		e := types.PublishBatchRequestEntry{
//			Id:      aws.String(f.UUID.String()),
//			Message: aws.String(fmt.Sprintf("%d", f.PackageId)),
//		}
//		snsEntries = append(snsEntries, e)
//
//		// Send SNS messages in blocks of batchSize
//		if len(snsEntries) == batchSize {
//			err := sendSNSMessages(snsEntries)
//			if err != nil {
//				return err
//			}
//			snsEntries = nil
//		}
//	}
//
//	// send remaining entries
//	err := sendSNSMessages(snsEntries)
//
//	return err
//}

//func sendSNSMessages(snsEntries []types.PublishBatchRequestEntry) error {
//	log.Debug("Number of SNS messages: ", len(snsEntries))
//
//	if len(snsEntries) > 0 {
//		params := sns.PublishBatchInput{
//			PublishBatchRequestEntries: snsEntries,
//			TopicArn:                   aws.String(manifestSession.SNSTopic),
//		}
//		_, err := manifestSession.SNSClient.PublishBatch(context.Background(), &params)
//		if err != nil {
//			log.Error("Error publishing to SNS: ", err)
//			return err
//		}
//	}
//
//	return nil
//
//}

// getPackageParams returns an array of PackageParams to insert in the Packages Table.
func getPackageParams(datasetId int, ownerId int, uploadFiles []uploadFile.UploadFile, pathToFolderMap models.PackageMap) ([]models.PackageParams, error) {
	var pkgParams []models.PackageParams

	// First create a map of params. As there can be upload-files that should be mapped to the same package,
	// we want to ensure we are not creating duplicate packages (as this will cause an error when inserting in db).
	// Then we turn map into array and return the array.
	pkgParamsMap := map[string]models.PackageParams{}
	for _, file := range uploadFiles {

		// Create the packageID based on the uploadID or the mergePackageID if it exists
		packageId, packageName, err := parsePackageId(file)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		parentId := int64(-1)
		if file.Path != "" {
			parentId = pathToFolderMap[file.Path].Id
		}

		uploadId := sql.NullString{
			String: file.UploadId,
			Valid:  true,
		}

		// Set Default attributes for File ==> Subtype and Icon
		var attributes []packageInfo.PackageAttribute
		attributes = append(attributes, packageInfo.PackageAttribute{
			Key:      "subtype",
			Fixed:    false,
			Value:    file.SubType,
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		}, packageInfo.PackageAttribute{
			Key:      "icon",
			Fixed:    false,
			Value:    file.Icon.String(),
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		})

		pkgParam := models.PackageParams{
			Name:         packageName,
			PackageType:  file.Type,
			PackageState: packageState.Uploaded,
			NodeId:       packageId,
			ParentId:     parentId,
			DatasetId:    datasetId,
			OwnerId:      ownerId,
			Size:         0,
			ImportId:     uploadId,
			Attributes:   attributes,
		}

		pkgParamsMap[packageId] = pkgParam
		//// If entry already exists --> sum size, else assign value
		//if val, ok := pkgParamsMap[packageId]; ok {
		//	val.Size += pkgParam.Size
		//} else {
		//	pkgParamsMap[packageId] = pkgParam
		//}

	}

	// Turn map into array --> ensure no duplicate packages.
	for i, _ := range pkgParamsMap {
		pkgParams = append(pkgParams, pkgParamsMap[i])
	}

	return pkgParams, nil

}

// parsePackageId returns a packageId and name based on upload-file
func parsePackageId(file uploadFile.UploadFile) (string, string, error) {
	packageId := fmt.Sprintf("N:package:%s", file.UploadId)
	packageName := file.Name
	if len(file.MergePackageId) > 0 {
		packageId = fmt.Sprintf("N:package:%s", file.MergePackageId)

		// Set packageName to name without extension
		r := regexp.MustCompile(`(?P<FileName>[^\.]*)?\.?(?P<Extension>.*)`)
		pathParts := r.FindStringSubmatch(file.Name)
		if pathParts == nil {
			log.Error("Unable to parse filename:", file.Name)
			return "", "", errors.New(fmt.Sprintf("Unable to parse filename: %s", file.Name))
		}

		packageName = pathParts[r.SubexpIndex("FileName")]
	}

	return packageId, packageName, nil
}
