package handler

import log "github.com/sirupsen/logrus"

type ArchiveEvent struct {
	ManifestId string `json:"manifest_id"`
}

func ManifestHandler(event ArchiveEvent) error {

	log.WithFields(
		log.Fields{
			"manifest_id": event.ManifestId,
		}).Info("Manifest Archiver called.")

	return nil
}
