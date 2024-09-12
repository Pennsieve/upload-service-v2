package test

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
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

func AssertExistsOneWhere(t *testing.T, db *sql.DB, orgID int, table string, where map[string]any) bool {
	qualifiedTable := fmt.Sprintf(`"%d".%s`, orgID, table)
	var conditions []string
	var args []any
	i := 1
	for columnName, value := range where {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", columnName, i))
		i++
		args = append(args, value)
	}
	whereClause := strings.Join(conditions, " AND ")
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, qualifiedTable, whereClause)

	row := db.QueryRow(query, args...)
	var count int
	err := row.Scan(&count)
	if assert.NoError(t, err) {
		return assert.Equal(t, 1, count, "expected table %s to have one match for %s, found %d instead",
			qualifiedTable, where, count)
	}
	return false
}
