package main

import (
	"database/sql"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type DBAndQueries struct {
	index   int
	db      *sql.DB
	queries *pgQueries.Queries
}

func TestPgManager_Locking(t *testing.T) {

	originalDB := &sql.DB{}
	pgManager := NewPgManager(originalDB)
	originalQueries := pgManager.Queries()

	newDB := &sql.DB{}
	require.NotSame(t, originalDB, newDB)
	require.Equal(t, originalDB, newDB)

	routines := 100
	resetRoutine := 50
	results := make(chan DBAndQueries)
	newQueriesChan := make(chan *pgQueries.Queries, 1)

	for i := 0; i < routines; i++ {
		go func(r int) {
			if r == resetRoutine {
				pgManager.Reset(newDB)
				assert.Same(t, newDB, pgManager.DB())
				newQueries := pgManager.Queries()
				assert.NotSame(t, originalQueries, newQueries)
				newQueriesChan <- newQueries
				close(newQueriesChan)
			}
			results <- DBAndQueries{
				index:   r,
				db:      pgManager.DB(),
				queries: pgManager.Queries(),
			}
		}(i)
	}

	// we expect a single newQueries
	allNewQueries := map[*pgQueries.Queries]bool{}

	for i := 0; i < routines; i++ {
		result := <-results
		if result.db == originalDB {
			assert.Same(t, originalQueries, result.queries)
		} else {
			assert.Same(t, newDB, result.db)
			assert.NotSame(t, originalQueries, result.queries)
			allNewQueries[result.queries] = true
		}
	}

	assert.Len(t, allNewQueries, 1)
	assert.Contains(t, allNewQueries, <-newQueriesChan)
}
