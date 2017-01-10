package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOffsets(t *testing.T) {
	os := offsets{}
	os.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", "topic", int32(0), 123)
	os.add("4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "", "topic2", int32(1), 456)
	os.add("16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "", "topic", int32(0), 124)
	os.add("EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "", "topic3", int32(2), 789)
	qs := os.flush()

	assert.Equal(t, 1, len(qs))
	assert.Equal(t, `INSERT INTO bookie.offset(fsmID, topic, topic_partition, startOffset, lastOffset, count, updated)`+
		` VALUES (?,?,?,?,?,?,?),(?,?,?,?,?,?,?),(?,?,?,?,?,?,?)`+
		` ON DUPLICATE KEY UPDATE lastOffset = VALUES(lastOffset), count = count + VALUES(count), updated = UTC_TIMESTAMP`, qs[0].sql, "sql mismatch")
	assert.True(t, containsSubSlice(qs[0].values, []interface{}{"16CCCB1C-BE16-4595-A610-6FC9E3F3CD80", "topic", int32(0), int64(123), int64(124), int64(2)}), "")
	assert.True(t, containsSubSlice(qs[0].values, []interface{}{"4CFEE785-1AEC-421E-AC6F-8314BDAF43C8", "topic2", int32(1), int64(456), int64(456), int64(1)}), "")
	assert.True(t, containsSubSlice(qs[0].values, []interface{}{"EFD6E9BD-3888-4CA9-B0D3-7E57BA64F9D5", "topic3", int32(2), int64(789), int64(789), int64(1)}), "")
	assert.Equal(t, 21, len(qs[0].values), "length mismatch")
	assert.Nil(t, os.o, "offsets was not cleared after flush")
}
