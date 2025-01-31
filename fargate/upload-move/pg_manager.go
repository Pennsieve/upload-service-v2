package main

import (
	"database/sql"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"sync"
)

type PgManager struct {
	pg    *pgQueries.Queries
	db    *sql.DB
	mutex sync.RWMutex
}

// NewPgManager should only be called from the main goroutine
func NewPgManager(db *sql.DB) *PgManager {
	return &PgManager{
		pg: pgQueries.New(db),
		db: db,
	}
}

func (m *PgManager) Queries() *pgQueries.Queries {
	m.mutex.RLock()
	defer func() {
		m.mutex.RUnlock()
	}()
	return m.pg
}

func (m *PgManager) DB() *sql.DB {
	m.mutex.RLock()
	defer func() {
		m.mutex.RUnlock()
	}()
	return m.db
}

func (m *PgManager) Reset(newDB *sql.DB) {
	m.mutex.Lock()
	defer func() {
		m.mutex.Unlock()
	}()
	m.db = newDB
	m.pg = pgQueries.New(newDB)
}
