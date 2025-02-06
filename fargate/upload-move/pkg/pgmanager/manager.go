package pgmanager

import (
	"context"
	"fmt"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type DBSupplier func() (DBApi, time.Duration, error)

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
func (m *PgManager) Queries() (*pgQueries.Queries, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.checkConnection(); err != nil {
		return nil, err
	}
	return m.pg, nil
}

func (m *PgManager) DB() (DBApi, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.checkConnection(); err != nil {
		return nil, err
	}
	return m.db, nil
}

func (m *PgManager) Close() error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.db.Close()
}

func (m *PgManager) checkConnection() error {
	ctx := context.Background()
	// If auth is unexpired and we can still ping re-use current pool
	now := time.Now()
	expired := now.Equal(m.authExpiration) || now.After(m.authExpiration)
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
