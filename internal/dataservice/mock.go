package dataservice

import (
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

func newMemoryMeta(allocator allocator) (*meta, error) {
	memoryKV := memkv.NewMemoryKV()
	return newMeta(memoryKV)
}

type MockAllocator struct {
	cnt int64
}

func (m *MockAllocator) allocTimestamp() (Timestamp, error) {
	val := atomic.AddInt64(&m.cnt, 1)
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return ts, nil
}

func (m *MockAllocator) allocID() (UniqueID, error) {
	val := atomic.AddInt64(&m.cnt, 1)
	return val, nil
}

func newMockAllocator() *MockAllocator {
	return &MockAllocator{}
}

func newTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "field1", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_STRING},
			{FieldID: 2, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_VECTOR_FLOAT},
		},
	}
}
