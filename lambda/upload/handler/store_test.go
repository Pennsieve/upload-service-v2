package handler

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	pgdbmodels "github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	testHelpers "github.com/pennsieve/pennsieve-go-core/test"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	"github.com/pusher/pusher-http-go/v5"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			mChangelogger := &test.MockChangelogger{}

			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3,
				ManifestFileTableName, ManifestTableName, SNSTopic, &pusher.Client{}, mChangelogger)

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

func TestStoreWithPG(t *testing.T) {
	client := getDynamoDBClient()

	pgdbClient, err := pgdb.ConnectENV()
	if err != nil {
		log.Fatal("cannot connect to db:", err)
	}

	mSNS := test.MockSNS{}
	mS3 := test.MockS3{}
	mChangelogger := &test.MockChangelogger{}
	mPusher := test.NewMockPusherClient()

	orgID := 1

	store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3,
		ManifestFileTableName, ManifestTableName, SNSTopic, mPusher, mChangelogger)
	require.NoError(t, store.WithOrg(orgID))

	for scenario, fn := range map[string]func(
		*testing.T, int, *UploadHandlerStore,
	){
		"import single file at top level":                 testImportFilesSingleFile,
		"import single file in folder at top level":       testImportFilesSingleFileInFolder,
		"import single file with leading slash":           testImportFilesSingleWithLeadingSlash,
		"import single file in folder with leading slash": testImportFilesSingleInFolderWithLeadingSlash,
	} {
		t.Run(scenario, func(t *testing.T) {
			t.Cleanup(func() {
				testHelpers.Truncate(t, pgdbClient, orgID, "packages")
				testHelpers.Truncate(t, pgdbClient, orgID, "files")
				testHelpers.Truncate(t, store.pgdb, orgID, "package_storage")
				testHelpers.Truncate(t, store.pgdb, orgID, "organization_storage")
				testHelpers.Truncate(t, store.pgdb, orgID, "dataset_storage")
				mChangelogger.Clear()
				mPusher.Clear()
			})

			fn(t, orgID, store)
		})
	}
}

func testImportFilesSingleFile(t *testing.T, orgID int, store *UploadHandlerStore) {
	datasetID := 1
	user := pgdbmodels.User{
		Id:           int64(1),
		NodeId:       "N:user:99f02be5-009c-4ecd-9006-f016d48628bf",
		Email:        uuid.NewString(),
		FirstName:    uuid.NewString(),
		LastName:     uuid.NewString(),
		IsSuperAdmin: false,
		PreferredOrg: int64(orgID),
	}
	manifestID := uuid.NewString()
	manifest := &dydb.ManifestTable{
		ManifestId:     manifestID,
		DatasetId:      int64(datasetID),
		DatasetNodeId:  uuid.NewString(),
		OrganizationId: int64(orgID),
		UserId:         user.Id,
		Status:         "",
		DateCreated:    0,
	}
	uploadID := uuid.NewString()
	files := []uploadFile.UploadFile{
		{
			ManifestId:     manifestID,
			UploadId:       uploadID,
			S3Bucket:       uuid.NewString(),
			S3Key:          fmt.Sprintf("%s/%s", manifestID, uploadID),
			Path:           "",
			Name:           "file1.txt",
			Extension:      "txt",
			FileType:       fileType.Text,
			Type:           packageType.Text,
			SubType:        "",
			Icon:           0,
			Size:           0,
			ETag:           "",
			MergePackageId: "",
			Sha256:         "",
		},
	}

	if assert.NoError(t, store.ImportFiles(context.Background(), datasetID, orgID, user, files, manifest)) {
		if test.AssertRowCount(t, store.pgdb, orgID, "packages", 1) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "packages", map[string]any{"name": files[0].Name})
		}
		if test.AssertRowCount(t, store.pgdb, orgID, "files", 1) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "files", map[string]any{"name": files[0].Name})
		}
	}

}

func testImportFilesSingleFileInFolder(t *testing.T, orgID int, store *UploadHandlerStore) {
	datasetID := 1
	user := pgdbmodels.User{
		Id:           int64(1),
		NodeId:       "N:user:99f02be5-009c-4ecd-9006-f016d48628bf",
		Email:        uuid.NewString(),
		FirstName:    uuid.NewString(),
		LastName:     uuid.NewString(),
		IsSuperAdmin: false,
		PreferredOrg: int64(orgID),
	}
	manifestID := uuid.NewString()
	manifest := &dydb.ManifestTable{
		ManifestId:     manifestID,
		DatasetId:      int64(datasetID),
		DatasetNodeId:  uuid.NewString(),
		OrganizationId: int64(orgID),
		UserId:         user.Id,
		Status:         "",
		DateCreated:    0,
	}
	uploadID := uuid.NewString()
	files := []uploadFile.UploadFile{
		{
			ManifestId:     manifestID,
			UploadId:       uploadID,
			S3Bucket:       uuid.NewString(),
			S3Key:          fmt.Sprintf("%s/%s", manifestID, uploadID),
			Path:           "dir1",
			Name:           "file1.txt",
			Extension:      "txt",
			FileType:       fileType.Text,
			Type:           packageType.Text,
			SubType:        "",
			Icon:           0,
			Size:           0,
			ETag:           "",
			MergePackageId: "",
			Sha256:         "",
		},
	}

	if assert.NoError(t, store.ImportFiles(context.Background(), datasetID, orgID, user, files, manifest)) {
		// One package for the folder and one for the file
		if test.AssertRowCount(t, store.pgdb, orgID, "packages", 2) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "packages", map[string]any{"name": "dir1"})
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "packages", map[string]any{"name": "file1.txt"})
		}
		if test.AssertRowCount(t, store.pgdb, orgID, "files", 1) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "files", map[string]any{"name": "file1.txt"})
		}
	}

}

func testImportFilesSingleWithLeadingSlash(t *testing.T, orgID int, store *UploadHandlerStore) {
	datasetID := 1
	user := pgdbmodels.User{
		Id:           int64(1),
		NodeId:       "N:user:99f02be5-009c-4ecd-9006-f016d48628bf",
		Email:        uuid.NewString(),
		FirstName:    uuid.NewString(),
		LastName:     uuid.NewString(),
		IsSuperAdmin: false,
		PreferredOrg: int64(orgID),
	}
	manifestID := uuid.NewString()
	manifest := &dydb.ManifestTable{
		ManifestId:     manifestID,
		DatasetId:      int64(datasetID),
		DatasetNodeId:  uuid.NewString(),
		OrganizationId: int64(orgID),
		UserId:         user.Id,
		Status:         "",
		DateCreated:    0,
	}
	uploadID := uuid.NewString()
	files := []uploadFile.UploadFile{
		{
			ManifestId:     manifestID,
			UploadId:       uploadID,
			S3Bucket:       uuid.NewString(),
			S3Key:          fmt.Sprintf("%s/%s", manifestID, uploadID),
			Path:           "/",
			Name:           "file1.txt",
			Extension:      "txt",
			FileType:       fileType.Text,
			Type:           packageType.Text,
			SubType:        "",
			Icon:           0,
			Size:           0,
			ETag:           "",
			MergePackageId: "",
			Sha256:         "",
		},
	}

	if assert.NoError(t, store.ImportFiles(context.Background(), datasetID, orgID, user, files, manifest)) {
		// There should only be one package since the path is "/", the import should not create a containing folder.
		if test.AssertRowCount(t, store.pgdb, orgID, "packages", 1) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "packages", map[string]any{"name": "file1.txt"})
		}
		if test.AssertRowCount(t, store.pgdb, orgID, "files", 1) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "files", map[string]any{"name": "file1.txt"})
		}
	}

}

func testImportFilesSingleInFolderWithLeadingSlash(t *testing.T, orgID int, store *UploadHandlerStore) {
	datasetID := 1
	user := pgdbmodels.User{
		Id:           int64(1),
		NodeId:       "N:user:99f02be5-009c-4ecd-9006-f016d48628bf",
		Email:        uuid.NewString(),
		FirstName:    uuid.NewString(),
		LastName:     uuid.NewString(),
		IsSuperAdmin: false,
		PreferredOrg: int64(orgID),
	}
	manifestID := uuid.NewString()
	manifest := &dydb.ManifestTable{
		ManifestId:     manifestID,
		DatasetId:      int64(datasetID),
		DatasetNodeId:  uuid.NewString(),
		OrganizationId: int64(orgID),
		UserId:         user.Id,
		Status:         "",
		DateCreated:    0,
	}
	uploadID := uuid.NewString()
	files := []uploadFile.UploadFile{
		{
			ManifestId:     manifestID,
			UploadId:       uploadID,
			S3Bucket:       uuid.NewString(),
			S3Key:          fmt.Sprintf("%s/%s", manifestID, uploadID),
			Path:           "/dir1",
			Name:           "file1.txt",
			Extension:      "txt",
			FileType:       fileType.Text,
			Type:           packageType.Text,
			SubType:        "",
			Icon:           0,
			Size:           0,
			ETag:           "",
			MergePackageId: "",
			Sha256:         "",
		},
	}

	if assert.NoError(t, store.ImportFiles(context.Background(), datasetID, orgID, user, files, manifest)) {
		// Two packages: dir1 and file1.txt
		if test.AssertRowCount(t, store.pgdb, orgID, "packages", 2) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "packages", map[string]any{"name": "dir1"})
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "packages", map[string]any{"name": "file1.txt"})
		}
		if test.AssertRowCount(t, store.pgdb, orgID, "files", 1) {
			test.AssertExistsOneWhere(t, store.pgdb, orgID, "files", map[string]any{"name": "file1.txt"})
		}
	}

}
