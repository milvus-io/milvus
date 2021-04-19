package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/stretchr/testify/assert"
)

func TestSizeofStruct(t *testing.T) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, baseEventHeader{})
	assert.Nil(t, err)
	s1 := binary.Size(baseEventHeader{})
	s2 := binary.Size(&baseEventHeader{})
	assert.Equal(t, s1, s2)
	assert.Equal(t, s1, buf.Len())
	buf.Reset()
	assert.Equal(t, 0, buf.Len())

	de := descriptorEventData{
		DescriptorEventDataFixPart: DescriptorEventDataFixPart{},
		PostHeaderLengths:          []uint8{0, 1, 2, 3},
	}
	err = de.Write(&buf)
	assert.Nil(t, err)
	s3 := binary.Size(de.DescriptorEventDataFixPart) + binary.Size(de.PostHeaderLengths)
	assert.Equal(t, s3, buf.Len())
}

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
	assert.EqualValues(t, length, insertEvent.EventLength)
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
