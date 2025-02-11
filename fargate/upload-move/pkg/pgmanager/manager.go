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
// is the time.Time that the returned DBApi expires.
type DBSupplier func() (DBApi, time.Time, error)

// PgManager is responsible for maintaining the PG related objects that the UploadMoveStore uses.
// Both a straight DBAPi and *pgdb.Queries.
// It maintains an expiration time for the current DBApi and checks this time before every call to DB()
// and Queries().
type PgManager struct {
	pg             *pgQueries.Queries
	db             DBApi
	mutex          sync.RWMutex
	dbSupplier     DBSupplier
	authExpiration time.Time
	alwaysPing     bool
}

// New should only be called from the main goroutine.
// supplier will be called when New is called and whenever the manager determines
// that the current DBApi needs to be replaced.
// If alwaysPing is false, then replacement is determined by the auth expiration time returned by
// supplier.
// If alwaysPing is true, then replacement is determined by the auth expiration time as well as a ping
func New(supplier DBSupplier, alwaysPing bool) (*PgManager, error) {
	db, authExpiration, err := supplier()
	if err != nil {
		return nil, fmt.Errorf("error creating initial connection pool: %w", err)
	}
	return &PgManager{
		pg:             pgQueries.New(db),
		db:             db,
		dbSupplier:     supplier,
		authExpiration: authExpiration,
		alwaysPing:     alwaysPing,
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
	expired := !time.Now().Before(m.authExpiration)
	var pingErr error
	if !expired && m.alwaysPing {
		pingErr = m.db.PingContext(ctx)
	}
	// if auth is unexpired and pingErr is nil for one reason or another, reuse current pool
	if !expired && pingErr == nil {
		return nil
	}

	log.WithFields(log.Fields{
		"alwaysPing": m.alwaysPing,
		"expired":    expired,
		"pingErr":    pingErr,
	}).Info("closing current connection pool and creating new one")

	// Close old pool and get a new one
	if err := m.db.Close(); err != nil {
		log.Warn("error closing expiring connection pool: ", err)
	}
	db, authExpiration, err := m.dbSupplier()
	if err != nil {
		return fmt.Errorf("error creating new connection pool: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("error connecting with new connection pool: %w", err)
	}
	m.authExpiration = authExpiration
	m.db = db
	m.pg = pgQueries.New(db)
	return nil
}
