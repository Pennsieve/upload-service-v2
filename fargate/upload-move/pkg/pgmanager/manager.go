package pgmanager

import (
	"context"
	"fmt"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// DBSupplier is called by New and then whenever the manager detects that it needs a new
// DBApi instance because the previous one has expired. The second return argument
// is the time.Duration that the returned DBApi is good for.
type DBSupplier func() (DBApi, time.Duration, error)

// PgManager is responsible for maintaining the PG related objects that the UploadMoveStore uses.
// Both a straight DBAPi and *pgdb.Queries
type PgManager struct {
	pg             *pgQueries.Queries
	db             DBApi
	mutex          sync.RWMutex
	dbSupplier     DBSupplier
	authExpiration time.Time
}

// New should only be called from the main goroutine.
func New(supplier DBSupplier) (*PgManager, error) {
	db, authDuration, err := supplier()
	if err != nil {
		return nil, fmt.Errorf("error creating initial connection pool: %w", err)
	}
	return &PgManager{
		pg:             pgQueries.New(db),
		db:             db,
		dbSupplier:     supplier,
		authExpiration: time.Now().Add(authDuration),
	}, nil
}

// Queries returns a *pgdb.Queries object backed by an un-expired connection pool
func (m *PgManager) Queries() (*pgQueries.Queries, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.checkConnection(); err != nil {
		return nil, err
	}
	return m.pg, nil
}

// DB returns a DBApi object backed by an un-expired connection pool
func (m *PgManager) DB() (DBApi, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.checkConnection(); err != nil {
		return nil, err
	}
	return m.db, nil
}

// Close closes the current DBApi
func (m *PgManager) Close() error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.db.Close()
}

// checkConnection calls should be protected by mutex.Lock() calls
func (m *PgManager) checkConnection() error {
	ctx := context.Background()
	// If auth is unexpired and if we can still ping, then re-use current pool
	expired := !time.Now().Before(m.authExpiration)
	pingErr := m.db.PingContext(ctx)
	if !expired && pingErr == nil {
		return nil
	}

	log.WithFields(log.Fields{
		"expired": expired,
		"pingErr": pingErr,
	}).Info("closing current connection pool and creating new one")

	// Close old pool and get a new one
	if err := m.db.Close(); err != nil {
		log.Warn("error closing expiring connection pool: ", err)
	}
	db, authDuration, err := m.dbSupplier()
	if err != nil {
		return fmt.Errorf("error creating new connection pool: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("error connecting with new connection pool: %w", err)
	}
	m.authExpiration = time.Now().Add(authDuration)
	m.db = db
	m.pg = pgQueries.New(db)
	return nil
}
