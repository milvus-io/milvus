package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestNewPayloadWriter(t *testing.T) {
	w, err := NewPayloadWriter(schemapb.DataType_STRING)
	assert.Nil(t, err)
	assert.NotNil(t, w)
	err = w.Close()
	assert.Nil(t, err)
}

func TestPayLoadString(t *testing.T) {
	w, err := NewPayloadWriter(schemapb.DataType_STRING)
	assert.Nil(t, err)
	err = w.AddOneStringToPayload("hello0")
	assert.Nil(t, err)
	err = w.AddOneStringToPayload("hello1")
	assert.Nil(t, err)
	err = w.AddOneStringToPayload("hello2")
	assert.Nil(t, err)
	err = w.FinishPayloadWriter()
	assert.Nil(t, err)
	length, err := w.GetPayloadLengthFromWriter()
	assert.Nil(t, err)
	assert.Equal(t, length, 3)
	buffer, err := w.GetPayloadBufferFromWriter()
	assert.Nil(t, err)

	r, err := NewPayloadReader(schemapb.DataType_STRING, buffer)
	assert.Nil(t, err)
	length, err = r.GetPayloadLengthFromReader()
	assert.Nil(t, err)
	assert.Equal(t, length, 3)
	str0, err := r.GetOneStringFromPayload(0)
	assert.Nil(t, err)
	assert.Equal(t, str0, "hello0")
	str1, err := r.GetOneStringFromPayload(1)
	assert.Nil(t, err)
	assert.Equal(t, str1, "hello1")
	str2, err := r.GetOneStringFromPayload(2)
	assert.Nil(t, err)
	assert.Equal(t, str2, "hello2")

	err = r.ReleasePayloadReader()
	assert.Nil(t, err)
	err = w.ReleasePayloadWriter()
	assert.Nil(t, err)
}
