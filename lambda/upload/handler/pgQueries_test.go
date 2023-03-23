package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	pgdb2 "github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	testHelpers "github.com/pennsieve/pennsieve-go-core/test"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestUploadService iterates over and runs tests.
func TestPgQueries(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadHandlerStore,
	){
		"test upload storage": testUpdateStorage,
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

func testUpdateStorage(t *testing.T, store *UploadHandlerStore) {

	orgId := 2
	datasetId := 1

	store.WithOrg(orgId)

	defer func() {
		testHelpers.Truncate(t, store.pgdb, orgId, "packages")
		testHelpers.Truncate(t, store.pgdb, orgId, "files")
		testHelpers.Truncate(t, store.pgdb, orgId, "package_storage")
		testHelpers.Truncate(t, store.pgdb, orgId, "organization_storage")
		testHelpers.Truncate(t, store.pgdb, orgId, "dataset_storage")
	}()

	// ADD FOLDER TO ROOT
	uploadId, _ := uuid.NewUUID()
	folderParams := pgdb2.PackageParams{
		Name:         "Folder1",
		PackageType:  packageType.Collection,
		PackageState: packageState.Ready,
		NodeId:       fmt.Sprintf("N:Package:%s", uploadId.String()),
		ParentId:     -1,
		DatasetId:    1,
		OwnerId:      1,
		Size:         1000, // should be ignored
		ImportId:     sql.NullString{String: uploadId.String(), Valid: true},
		Attributes:   []packageInfo.PackageAttribute{},
	}

	folder1, err := store.pg.AddFolder(context.Background(), folderParams)
	assert.NoError(t, err)

	// ADD NESTED FOLDER
	uploadId, _ = uuid.NewUUID()
	folderParams = pgdb2.PackageParams{
		Name:         "Folder2",
		PackageType:  packageType.Collection,
		PackageState: packageState.Ready,
		NodeId:       fmt.Sprintf("N:Package:%s", uploadId.String()),
		ParentId:     folder1.Id,
		DatasetId:    1,
		OwnerId:      1,
		Size:         1000, // should be ignored
		ImportId:     sql.NullString{String: uploadId.String(), Valid: true},
		Attributes:   []packageInfo.PackageAttribute{},
	}

	folder2, err := store.pg.AddFolder(context.Background(), folderParams)
	assert.NoError(t, err)

	// Test adding packages to root
	testParams := []testHelpers.PackageParams{
		{Name: "package_1.txt", ParentId: -1, NodeId: "N:Package:1"},
		{Name: "package_2.txt", ParentId: -1, NodeId: "N:Package:2"},
		{Name: "package_3.txt", ParentId: folder1.Id, NodeId: "N:Package:3"},
		{Name: "package_4.txt", ParentId: folder2.Id, NodeId: "N:Package:4"},
		{Name: "package_5.txt", ParentId: folder2.Id, NodeId: "N:Package:5"},
		{Name: "package_5.txt", ParentId: folder2.Id, NodeId: "N:Package:6"},
		{Name: "package_5.txt", ParentId: folder2.Id, NodeId: "N:Package:7"},
	}

	insertParams := testHelpers.GenerateTestPackages(testParams, 1)
	results, err := store.pg.AddPackages(context.Background(), insertParams)
	assert.NoError(t, err)

	packageByNodeIdMap := map[string]pgdb2.Package{}
	for _, p := range results {
		packageByNodeIdMap[p.NodeId] = p
	}

	files := []pgdb2.FileParams{
		{PackageId: int(packageByNodeIdMap["N:Package:1"].Id), Size: 10},
		{PackageId: int(packageByNodeIdMap["N:Package:3"].Id), Size: 10},
		{PackageId: int(packageByNodeIdMap["N:Package:7"].Id), Size: 10},
	}

	err = store.pg.UpdateStorage(files, results, int64(datasetId), int64(orgId))
	assert.NoError(t, err, "Updating package, dataset, org size should succeed")

	ctx := context.Background()
	package1Size, err := store.pg.GetPackageStorageById(ctx, packageByNodeIdMap["N:Package:1"].Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), package1Size, "Package size should equal the set value")

	// Get Folder1 size
	folder1Size, err := store.pg.GetPackageStorageById(ctx, folder1.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), folder1Size, "Folder1 size should equal sum of package 3 and 7")

	// Get Dataset size
	datasetSize, err := store.pg.GetDatasetStorageById(ctx, int64(datasetId))
	assert.NoError(t, err)
	assert.Equal(t, int64(30), datasetSize, "datasetSize size should equal sum of package 1, 3 and 7")

	// Get Organization size
	orgSize, err := store.pg.GetOrganizationStorageById(ctx, int64(orgId))
	assert.NoError(t, err)
	assert.Equal(t, int64(30), orgSize, "Org size should equal sum of package 1, 3 and 7")
}
