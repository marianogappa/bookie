package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTags(t *testing.T) {
	ts := tags{}
	ts.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "created", "123456789")
	ts.add("4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "created", "234567898")
	ts.add("EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "created", "678909034")
	query := ts.flush()

	assert.Equal(t, `INSERT INTO bookie.tags(fsmID, k, v) VALUES (?,?,?),(?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE v = VALUES(v)`, query.sql, "sql mismatch")
	assert.True(t, containsSubSlice(query.values, []interface{}{"16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "created", "123456789"}), "")
	assert.True(t, containsSubSlice(query.values, []interface{}{"4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "created", "234567898"}), "")
	assert.True(t, containsSubSlice(query.values, []interface{}{"EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "created", "678909034"}), "")
	assert.Equal(t, len(query.values), 9, "length mismatch")
	assert.Nil(t, ts.t, "tags was not cleared after flush")
}

func containsSubSlice(sl, subsl []interface{}) bool {
	j := 0
	for i := range sl {
		if subsl[j] == sl[i] {
			j++
			if j == len(subsl) {
				return true
			}
			continue
		}
		j = 0
	}
	return false
}
