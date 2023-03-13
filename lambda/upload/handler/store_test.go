package handler

import (
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStore(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, store *UploadHandlerStore,
	){
		"sorts upload files by path":           testSortFiles,
		"correctly maps files to folders":      testFolderMapping,
		"test ignore leading slash":            testRemoveLeadingTrailingSlash,
		"test folder mapping for nested files": testNestedStructure,
		"test deleting orphaned files":         testDeleteOrphanedFiles,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getDynamoDBClient()

			pgdbClient, err := pgdb.ConnectENV()
			if err != nil {
				log.Fatal("cannot connect to db:", err)
			}

			mSNS := test.MockSNS{}
			mS3 := test.MockS3{}
			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, ManifestFileTableName, ManifestTableName, SNSTopic)

			fn(t, store)
		})
	}
}

func testSortFiles(t *testing.T, _ *UploadHandlerStore) {

	uploadFile1 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/asd/123/",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile2 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/asd/123",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}

	uploadFiles := []uploadFile.UploadFile{
		uploadFile1,
		uploadFile2,
	}

	var u uploadFile.UploadFile
	u.Sort(uploadFiles)
	assert.Equal(t, uploadFiles[0], uploadFile2)

}

func testFolderMapping(t *testing.T, _ *UploadHandlerStore) {

	uploadFile1 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/folder2/folder3",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile2 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder1/folder10",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile3 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "folder2/folder1/folder8",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}

	uploadFiles := []uploadFile.UploadFile{
		uploadFile1,
		uploadFile2,
		uploadFile3,
	}

	folderMap := getUploadFolderMap(uploadFiles, "")

	// Number of folders
	assert.Equal(t, 7, len(folderMap))

	// Check Folder exists
	assert.True(t, folderMap["folder1/folder10"] != nil)
	assert.True(t, folderMap["folder1/unknownFolder"] == nil)

	// Check Folder Parents
	assert.Equal(t, folderMap["folder1/folder10"].ParentNodeId, folderMap["folder1"].NodeId)
	assert.Equal(t, folderMap["folder1/folder2/folder3"].ParentNodeId, folderMap["folder1/folder2"].NodeId)
	assert.Equal(t, folderMap["folder1/folder10"].ParentNodeId, folderMap["folder1/folder2"].ParentNodeId)

	// Check folder depth
	assert.Equal(t, 0, folderMap["folder1"].Depth)
	assert.Equal(t, 2, folderMap["folder1/folder2/folder3"].Depth)

	// Check population of children in parents
	assert.Contains(t, folderMap["folder1"].Children, folderMap["folder1/folder10"])
	assert.Contains(t, folderMap["folder1"].Children, folderMap["folder1/folder2"])
	assert.NotContains(t, folderMap["folder1"].Children, folderMap["folder2/folder1"])

	//** Check with alternative root folder.

	folderMap2 := getUploadFolderMap(uploadFiles, "hello/you")

	// Number of folders
	assert.Equal(t, 9, len(folderMap2))

	// Check Folder exists
	assert.True(t, folderMap2["hello/you/folder1/folder10"] != nil)
	assert.True(t, folderMap2["hello/you/folder1/unknownFolder"] == nil)

	// Check Folder Parents
	assert.Equal(t, folderMap2["hello/you/folder1/folder10"].ParentNodeId, folderMap2["hello/you/folder1"].NodeId)
	assert.Equal(t, folderMap2["hello/you/folder1/folder2/folder3"].ParentNodeId, folderMap2["hello/you/folder1/folder2"].NodeId)
	assert.Equal(t, folderMap2["hello/you/folder1/folder10"].ParentNodeId, folderMap2["hello/you/folder1/folder2"].ParentNodeId)

	// Check folder depth
	assert.Equal(t, 2, folderMap2["hello/you/folder1"].Depth)
	assert.Equal(t, 4, folderMap2["hello/you/folder1/folder2/folder3"].Depth)

}

func testRemoveLeadingTrailingSlash(t *testing.T, _ *UploadHandlerStore) {
	uploadFile1 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "/folder1/folder2",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile2 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "/////folder1/folder10",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile3 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "/folder1/folder10///",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}
	uploadFile4 := uploadFile.UploadFile{
		ManifestId: "",
		Path:       "/folder1/folder10/",
		Name:       "",
		Extension:  "",
		Type:       0,
		SubType:    "",
		Icon:       0,
		Size:       0,
		ETag:       "",
	}

	uploadFiles := []uploadFile.UploadFile{
		uploadFile1,
		uploadFile2,
		uploadFile3,
		uploadFile4,
	}

	folderMap := getUploadFolderMap(uploadFiles, "")

	t.Log(folderMap)
	// Number of folders
	assert.Equal(t, 3, len(folderMap))

}

func testNestedStructure(t *testing.T, _ *UploadHandlerStore) {

	files := []uploadFile.UploadFile{
		{Path: "protocol_1/protocol_2", Name: "Readme.md"},
		{Path: "", Name: "manifest.xlsx"},
		{Path: "protocol_1", Name: "manifest.xlsx"},
		{Path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5", Name: "manifest.xlsx"},
		{Path: "protocol_1/protocol_2", Name: "manifest.xlsx"},
		{Path: "protocol_1/protocol_2/protocol_3", Name: "manifest.xlsx"},
		{Path: "protocol_1/protocol_2/protocol_3/protocol_4", Name: "Readme.md"},
		{Path: "protocol_1/protocol_2/protocol_3/protocol_4", Name: "manifest.xlsx"},
		{Path: "", Name: "Readme.md"},
		{Path: "protocol_1", Name: "Readme.md"},
		{Path: "protocol_1/protocol_2/protocol_3", Name: "Readme.md"},
		{Path: "protocol_1/protocol_2/protocol_3/protocol_4/protocol_5", Name: "Readme.md"},
	}

	var u uploadFile.UploadFile
	u.Sort(files)

	uploadMap := getUploadFolderMap(files, "")

	assert.Empty(t, uploadMap["protocol_1"].ParentNodeId)
	assert.Equal(t, uploadMap["protocol_1"].NodeId, uploadMap["protocol_1/protocol_2"].ParentNodeId)
	assert.Equal(t, uploadMap["protocol_1/protocol_2"].NodeId, uploadMap["protocol_1/protocol_2/protocol_3"].ParentNodeId)
	assert.Equal(t, uploadMap["protocol_1/protocol_2/protocol_3"].NodeId, uploadMap["protocol_1/protocol_2/protocol_3/protocol_4"].ParentNodeId)
	assert.Equal(t, uploadMap["protocol_1/protocol_2/protocol_3/protocol_4"].NodeId, uploadMap["protocol_1/protocol_2/protocol_3/protocol_4/protocol_5"].ParentNodeId)

}

func testDeleteOrphanedFiles(t *testing.T, store *UploadHandlerStore) {
	// This test only tests the method argument parsing as the S3 client is mocked

	files := []OrphanS3File{
		{
			S3Bucket: "123",
			S3Key:    "123",
		},
		{
			S3Bucket: "123",
			S3Key:    "456",
		},
	}

	err := store.deleteOrphanFiles(files)
	assert.NoError(t, err)

}
