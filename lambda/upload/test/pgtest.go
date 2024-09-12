package test

import (
	"database/sql"
	"errors"
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
		var condition string
		if value == nil {
			condition = fmt.Sprintf("%s IS NULL", columnName)
		} else {
			condition = fmt.Sprintf("%s = $%d", columnName, i)
			i++
			args = append(args, value)

		}
		conditions = append(conditions, condition)
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

// AssertPackageIsChild compares packages based on name for convenience but this means it may not work if there are multiple
// packages with the same name
func AssertPackageIsChild(t *testing.T, db *sql.DB, orgID int, childName string, expectedParentName string) bool {

	qualifiedTable := fmt.Sprintf(`"%d".%s`, orgID, "packages")
	query := fmt.Sprintf(`SELECT TRUE FROM %s child JOIN %s parent ON child.parent_id = parent.id
                                         WHERE child.name = '%s' AND parent.name = '%s'`,
		qualifiedTable,
		qualifiedTable,
		childName,
		expectedParentName)

	row := db.QueryRow(query)
	var isChild bool
	if err := row.Scan(&isChild); errors.Is(err, sql.ErrNoRows) {
		return assert.Fail(t, "package is not child of expected parent", "expected package %s to be child of %s in table %s",
			childName, expectedParentName, qualifiedTable)
	} else if assert.NoError(t, err) {
		return assert.True(t, isChild, "expected package %s to be child of %s in table %s",
			childName, expectedParentName, qualifiedTable)
	}
	return false
}
