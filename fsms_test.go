package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFSMs(t *testing.T) {
	fs := fsms{}
	fs.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", "2006-01-02", "2006-01-02")
	fs.add("4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "", "2006-03-04", "2006-01-02")
	fs.add("EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "", "2006-05-06", "2006-01-02")
	query := fs.flush()

	assert.Equal(t, `INSERT INTO bookie.fsm(fsmID, fsmAlias, created) VALUES (?,?,?),(?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE created = LEAST(created, VALUES(created))`, query.sql, "sql mismatch")
	assert.True(t, containsSubSlice(query.values, []interface{}{"16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", timeParse("2006-01-02", "2006-01-02")}), "")
	assert.True(t, containsSubSlice(query.values, []interface{}{"4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "", timeParse("2006-01-02", "2006-03-04")}), "")
	assert.True(t, containsSubSlice(query.values, []interface{}{"EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "", timeParse("2006-01-02", "2006-05-06")}), "")
	assert.Equal(t, len(query.values), 9, "length mismatch")
	assert.Nil(t, fs.t, "fsms was not cleared after flush")
}

func timeParse(l, s string) time.Time {
	t, _ := time.Parse(l, s)
	return t
}
