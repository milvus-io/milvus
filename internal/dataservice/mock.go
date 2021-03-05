package dataservice

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

func newMemoryMeta(allocator allocatorInterface) (*meta, error) {
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

type mockDataNodeClient struct {
}

func newMockDataNodeClient() *mockDataNodeClient {
	return &mockDataNodeClient{}
}

func (c *mockDataNodeClient) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}, nil
}

func (c *mockDataNodeClient) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	// todo
	return nil, nil
}

func (c *mockDataNodeClient) FlushSegments(ctx context.Context, in *datapb.FlushSegRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}, nil
}

func (c *mockDataNodeClient) Stop() error {
	return nil
}
