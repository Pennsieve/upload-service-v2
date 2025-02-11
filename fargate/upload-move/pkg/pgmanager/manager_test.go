package pgmanager

import (
	"context"
	"database/sql"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const initialAuthDuration = 500 * time.Millisecond

type testSupplierWrapper struct {
	callCount atomic.Int32
}

// supply returns initialAuthDuration on first call
// and then returns longer durations after that.
// Idea is that the first one is short since we
// sleep for that length to let the db expire before testing access.
// And then subsequent durations are more realistic where a db will not
// expire between calling PgManager.DB() or PgManager.Queries() and use
func (t *testSupplierWrapper) supply() (DBApi, time.Duration, error) {
	time.Sleep(time.Duration(rand.Int63n(500)+1) * time.Millisecond)
	authDuration := initialAuthDuration
	callCount := t.callCount.Add(1)
	if callCount > 1 {
		authDuration = 10 * time.Second
	}
	return &MockDBApi{}, authDuration, nil
}

func TestPgManager_Locking(t *testing.T) {
	for scenario, alwaysPing := range map[string]bool{
		"ping as part of check":       true,
		"don't ping as part of check": false,
	} {
		t.Run(scenario, func(t *testing.T) {
			supplierWrapper := &testSupplierWrapper{}
			manager, err := New(supplierWrapper.supply, alwaysPing)
			require.NoError(t, err)

			// Wait for the initial DB to expire
			time.Sleep(initialAuthDuration)

			routines := 1_000

			ctx := context.Background()
			var wg sync.WaitGroup

			for i := 0; i < routines; i++ {
				wg.Add(1)
				go func(r int) {
					defer wg.Done()
					db, err := manager.DB()
					if assert.NoError(t, err) {
						assert.False(t, db.(*MockDBApi).IsExpired())
					}
					queries, err := manager.Queries()
					if assert.NoError(t, err) {
						// indirectly test if db underlying queries is expired
						require.NoError(t, queries.UpdateBucketForFile(ctx, "", "", "", 0))
					}
				}(i)
			}
			wg.Wait()
			// Expect the call on installation of the manager and then one once the initialAuthDuration runs out.,
			assert.Equal(t, int32(2), supplierWrapper.callCount.Load())
		})
	}
}

type MockDBApi struct {
	isExpired bool
	mutex     sync.RWMutex
}

func (m *MockDBApi) Expire() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.isExpired = true
}

func (m *MockDBApi) IsExpired() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.isExpired
}

func (m *MockDBApi) ExecContext(ctx context.Context, s string, i ...interface{}) (sql.Result, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.isExpired {
		return nil, errors.New("expired")
	}
	return &MockSqlResult{}, nil
}

func (m *MockDBApi) PrepareContext(ctx context.Context, s string) (*sql.Stmt, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.isExpired {
		return nil, errors.New("expired")
	}
	return nil, nil
}

func (m *MockDBApi) QueryContext(ctx context.Context, s string, i ...interface{}) (*sql.Rows, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.isExpired {
		return nil, errors.New("expired")
	}
	return nil, nil
}

func (m *MockDBApi) QueryRowContext(ctx context.Context, s string, i ...interface{}) *sql.Row {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.isExpired {
		return nil
	}
	return &sql.Row{}
}

func (m *MockDBApi) PingContext(ctx context.Context) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.isExpired {
		return errors.New("expired")
	}
	return nil
}

func (m *MockDBApi) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	panic("implement me if you need this")
}

func (m *MockDBApi) Close() error {
	m.Expire()
	return nil
}

type MockSqlResult struct{}

func (m *MockSqlResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (m *MockSqlResult) RowsAffected() (int64, error) {
	return 1, nil
}
