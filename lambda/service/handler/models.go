package handler

type ArchivePostResponse struct {
	Message    string `json:"message"`
	ManifestId string `json:"manifest_id"'`
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
