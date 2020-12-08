package storage

import (
	"bytes"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/stretchr/testify/assert"
)

func TestEventWriter(t *testing.T) {
	insertEvent, err := newInsertEventWriter(schemapb.DataType_INT32, 0)
	assert.Nil(t, err)
	defer insertEvent.Close()
	err = insertEvent.Close()
	assert.Nil(t, err)

	insertEvent, err = newInsertEventWriter(schemapb.DataType_INT32, 0)
	assert.Nil(t, err)
	err = insertEvent.AddInt64ToPayload([]int64{1, 1})
	assert.NotNil(t, err)
	err = insertEvent.AddInt32ToPayload([]int32{1, 2, 3})
	assert.Nil(t, err)
	nums, err := insertEvent.GetPayloadLengthFromWriter()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)
	err = insertEvent.Finish()
	assert.Nil(t, err)
	length, err := insertEvent.GetMemoryUsageInBytes()
	assert.Nil(t, err)
	assert.EqualValues(t, length, insertEvent.eventLength)
	err = insertEvent.AddInt32ToPayload([]int32{1})
	assert.NotNil(t, err)
	buffer := new(bytes.Buffer)
	err = insertEvent.Write(buffer)
	assert.Nil(t, err)
	length, err = insertEvent.GetMemoryUsageInBytes()
	assert.Nil(t, err)
	assert.EqualValues(t, length, buffer.Len())
	err = insertEvent.Close()
	assert.Nil(t, err)

}
