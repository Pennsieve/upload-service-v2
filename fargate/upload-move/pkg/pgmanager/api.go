package pgmanager

import (
	"context"
	"database/sql"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"time"
)

// DBApi is an interface that combines the direct sql.DB methods
// used by the UploadMoveStore and those needed to instantiate a
// pgdb.Queries also used by the UploadMoveStore
type DBApi interface {
	pgQueries.DBTX
	PingContext(ctx context.Context) error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

// NewDBApi creates a client connection pool to the RDS proxy and returns this along
// with an authDuration of 10 minutes. This is to keep the expiration time well below
// the expiration time of the RDS auth token which is 15 minutes.
// It will be used by PgManager when the manager is first created and also to reset
// if the manager detects that its current pool has expired.
func NewDBApi() (DBApi, time.Duration, error) {
	db, err := pgQueries.ConnectRDS()
	if err != nil {
		return nil, 0, err
	}
	return db, 10 * time.Minute, nil
}
