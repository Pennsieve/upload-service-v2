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
// with an authExpiration 10 minutes in the future. This is to keep the expiration time well below
// the expiration time of the RDS auth token which is 15 minutes.
// It will be used by PgManager when the manager is first created and also to reset
// if the manager detects that its current pool has expired.
func NewDBApi() (DBApi, time.Time, error) {
	now := time.Now()
	db, err := pgQueries.ConnectRDS()
	if err != nil {
		return nil, now, err
	}
	return db, now.Add(10 * time.Minute), nil
}
