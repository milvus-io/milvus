package collection

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

var (
	cid        = UniqueID(10011111234)
	name       = "test-segment"
	createTime = time.Now()
	schema     = []*schemapb.FieldSchema{}
	sIDs       = []UniqueID{111111, 222222}
	ptags      = []string{"default", "test"}
)

func TestNewCollection(t *testing.T) {
	assert := assert.New(t)
	c := NewCollection(cid, name, createTime, schema, sIDs, ptags)
	assert.Equal(cid, c.ID)
	assert.Equal(name, c.Name)
	for k, v := range schema {
		assert.Equal(v.Name, c.Schema[k].FieldName)
		assert.Equal(v.DataType, c.Schema[k].Type)
	}
	assert.Equal(sIDs, c.SegmentIDs)
	assert.Equal(ptags, c.PartitionTags)
}

func TestGrpcMarshal(t *testing.T) {
	assert := assert.New(t)
	c := NewCollection(cid, name, createTime, schema, sIDs, ptags)
	newc := GrpcMarshal(&c)
	assert.NotEqual("", newc.GrpcMarshalString)
}
