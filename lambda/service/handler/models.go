package handler

import "fmt"

type ArchivePostResponse struct {
	Message    string `json:"message"`
	ManifestId string `json:"manifest_id"'`
}

type ArchiveDeleteResponse struct {
	Message string `json:"message"`
}

type ArchiveGetResponse struct {
	Message string `json:"message"`
	Url     string `json:"url"'`
}

type ArchiveEvent struct {
	ManifestId     string `json:"manifest_id"`
	OrganizationId int64  `json:"organization_id"`
	DatasetId      int64  `json:"dataset_id"`
}

type ManifestNotExistError struct {
	id string
}

func (e *ManifestNotExistError) Error() string {
	return fmt.Sprintf("manifest with id %s does not exist", e.id)
}

type ManifestNotArchivedError struct {
	id     string
	status string
}

func (e *ManifestNotArchivedError) Error() string {
	return fmt.Sprintf("manifest with id %s is not archived (%s)", e.id, e.status)
}
