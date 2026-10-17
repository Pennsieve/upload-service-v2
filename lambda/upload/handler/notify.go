package handler

import (
	"context"
	"strings"

	"github.com/pennsieve/pennsieve-go-core/pkg/realtime"
	log "github.com/sirupsen/logrus"
)

const uploadEventName = "upload-event"

// maxRowsPerUploadEvent keeps each AppSync event well under its 240 KB
// delivered-message limit (a row is ~200 bytes). Subscribers only use the event
// to refresh the file list, so splitting a batch across events is harmless.
const maxRowsPerUploadEvent = 500

// WithRealtime makes the store publish upload events to the AppSync Event API
// instead of Pusher. Set only where REALTIME_EVENTS_ENDPOINT is configured.
func (s *UploadHandlerStore) WithRealtime(p realtime.Publisher) *UploadHandlerStore {
	s.publisher = p
	return s
}

// notifyUpload tells subscribers of the dataset which packages just landed.
// Failures are logged: nobody's upload fails over a live update.
func (s *UploadHandlerStore) notifyUpload(ctx context.Context, datasetNodeId string, rows []uploadPusherItem) {
	if s.publisher != nil {
		ch := realtime.Dataset(datasetNodeId)
		var events []realtime.Event
		for start := 0; ; start += maxRowsPerUploadEvent {
			end := min(start+maxRowsPerUploadEvent, len(rows))
			events = append(events, realtime.Event{Channel: ch, Name: uploadEventName, Data: rows[start:end]})
			if end >= len(rows) {
				break
			}
		}
		if err := s.publisher.PublishBatch(ctx, events); err != nil {
			log.WithField("dataset", datasetNodeId).Warnf("realtime: publishing upload event: %v", err)
		}
		return
	}

	if s.pusherClient == nil {
		return
	}
	chName := strings.ReplaceAll(datasetNodeId, "N:dataset:", "dataset-")
	if err := s.pusherClient.Trigger(chName, uploadEventName, rows); err != nil {
		log.Warn(err)
	}
}
