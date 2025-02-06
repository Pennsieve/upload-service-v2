package pgmanager

import (
	"context"
	"database/sql"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"time"
)

type DBApi interface {
	pgQueries.DBTX
	PingContext(ctx context.Context) error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

func NewDBApi() (DBApi, time.Duration, error) {
	db, err := pgQueries.ConnectRDS()
	if err != nil {
		return nil, 0, err
	}
	return db, 10 * time.Minute, nil
}
