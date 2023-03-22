package handler

type ArchivePostResponse struct {
	Message    string `json:"message"`
	ManifestId string `json:"manifest_id"'`
}

type ArchiveEvent struct {
	ManifestId     string `json:"manifest_id"`
	OrganizationId int64  `json:"organization_id"`
	DatasetId      int64  `json:"dataset_id"`
}
