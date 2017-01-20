package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAccumulators(t *testing.T) {
	as := accumulators{}
	as.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", "counter", float64(1))
	as.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", "counter", float64(1))
	as.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", "other_counter", float64(1))
	as.add("4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "", "accumulator", float64(5))
	as.add("4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "", "accumulator", float64(8))
	as.add("EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "", "decimal_accumulator", float64(1.5))
	as.add("EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "", "decimal_accumulator", float64(-0.3))
	as.add("", "ABC6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "incomplete_accumulator", float64(123.456))
	qs := as.flush()

	assert.Equal(t, 2, len(qs))
	assert.Equal(t, `INSERT INTO bookie.tmpAccumulators(fsmAlias, k, v) VALUES (?,?,?) ON DUPLICATE KEY UPDATE v = v + VALUES(v)`, qs[0].sql, "sql mismatch")
	assert.True(t, containsSubSlice(qs[0].values, []interface{}{"ABC6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "incomplete_accumulator", float64(123.456)}), "")
	assert.Equal(t, 3, len(qs[0].values), "length mismatch")
	assert.Equal(t, `INSERT INTO bookie.accumulators(fsmID, k, v) VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE v = v + VALUES(v)`, qs[1].sql, "sql mismatch")
	assert.True(t, containsSubSlice(qs[1].values, []interface{}{"16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "counter", float64(2)}), "")
	assert.True(t, containsSubSlice(qs[1].values, []interface{}{"16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "other_counter", float64(1)}), "")
	assert.True(t, containsSubSlice(qs[1].values, []interface{}{"4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "accumulator", float64(13)}), "")
	assert.True(t, containsSubSlice(qs[1].values, []interface{}{"EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "decimal_accumulator", float64(1.2)}), "")
	assert.Equal(t, 12, len(qs[1].values), "length mismatch")
	assert.Nil(t, as.t, "accumulators was not cleared after flush")
}
