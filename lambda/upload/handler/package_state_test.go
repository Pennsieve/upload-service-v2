package handler

import (
	"testing"

	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every uploaded package starts UPLOADED (waiting for its virus scan), whatever its
// type: READY means cleared by scan-service, not processed. Types that used to be
// READY straight away (text, ...) and types go-core flags HasWorkflow (images,
// timeseries, MRI, slides, video) are treated alike.
func TestGetPackageParamsAlwaysUploaded(t *testing.T) {
	files := []uploadFile.UploadFile{
		{UploadId: "11111111-1111-1111-1111-111111111111", Name: "plot.png", FileType: fileType.PNG, Type: packageType.Image},
		{UploadId: "22222222-2222-2222-2222-222222222222", Name: "photo.jpg", FileType: fileType.JPEG, Type: packageType.Image},
		{UploadId: "33333333-3333-3333-3333-333333333333", Name: "recording.edf", FileType: fileType.EDF, Type: packageType.TimeSeries},
		{UploadId: "44444444-4444-4444-4444-444444444444", Name: "brain.nii", FileType: fileType.NIFTI, Type: packageType.MRI},
		{UploadId: "55555555-5555-5555-5555-555555555555", Name: "slide.svs", FileType: fileType.Aperio, Type: packageType.Slide},
		{UploadId: "66666666-6666-6666-6666-666666666666", Name: "clip.mp4", FileType: fileType.MP4, Type: packageType.Video},
		{UploadId: "77777777-7777-7777-7777-777777777777", Name: "notes.txt", FileType: fileType.Text, Type: packageType.Text},
	}

	params, err := getPackageParams(1, 1, files, nil)
	require.NoError(t, err)
	require.Len(t, params, len(files))

	for _, p := range params {
		assert.Equal(t, packageState.Uploaded, p.PackageState, "%s (%s)", p.Name, p.PackageType)
	}
}
