package handler

import (
	"context"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
)

type Changelogger interface {
	EmitEvents(ctx context.Context, params changelog.Message) error
}
