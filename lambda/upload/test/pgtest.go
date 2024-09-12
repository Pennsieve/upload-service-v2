package test

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func AssertRowCount(t *testing.T, db *sql.DB, orgID int, table string, expectedRowCount int) bool {

	qualifiedTable := fmt.Sprintf(`"%d".%s`, orgID, table)
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, qualifiedTable)

	row := db.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if assert.NoError(t, err) {
		return assert.Equal(t, expectedRowCount, count, "expected table %s to have %d rows, found %d instead",
			qualifiedTable, expectedRowCount, count)
	}
	return false
}
