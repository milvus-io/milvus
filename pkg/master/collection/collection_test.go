package collection

import (
	"testing"
	"time"

	messagepb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/stretchr/testify/assert"
)

var (
	cid        = uint64(10011111234)
	name       = "test-segment"
	createTime = time.Now()
	schema     = []*messagepb.FieldMeta{}
	sIds       = []uint64{111111, 222222}
	ptags      = []string{"default", "test"}
)

func TestNewCollection(t *testing.T) {
	assert := assert.New(t)
	c := NewCollection(cid, name, createTime, schema, sIds, ptags)
	assert.Equal(cid, c.ID)
	assert.Equal(name, c.Name)
	for k, v := range schema {
		assert.Equal(v.Dim, c.Schema[k].DIM)
		assert.Equal(v.FieldName, c.Schema[k].FieldName)
		assert.Equal(v.Type, c.Schema[k].Type)
	}
	assert.Equal(sIds, c.SegmentIDs)
	assert.Equal(ptags, c.PartitionTags)
}

func TestGrpcMarshal(t *testing.T) {
	assert := assert.New(t)
	c := NewCollection(cid, name, createTime, schema, sIds, ptags)
	newc := GrpcMarshal(&c)
	assert.NotEqual("", newc.GrpcMarshalString)
}
