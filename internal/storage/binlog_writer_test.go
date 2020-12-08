package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestBinlogWriter(t *testing.T) {
	binlogWriter := NewInsertBinlogWriter(schemapb.DataType_INT32)
	defer binlogWriter.Close()
	eventWriter, err := binlogWriter.NextInsertEventWriter()
	assert.Nil(t, err)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3})
	assert.Nil(t, err)
	assert.Nil(t, nil, binlogWriter.GetBuffer())
	err = binlogWriter.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, binlogWriter.GetEventNums())
	nums, err := binlogWriter.GetRowNums()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3})
	assert.NotNil(t, err)
	nums, err = binlogWriter.GetRowNums()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)
}
