package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func TestMaster_CreateCollectionTask(t *testing.T) {
	req := internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		Schema:    nil,
	}
	var collectionTask task = &createCollectionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kCreateCollection, collectionTask.Type())
	ts, err := collectionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	collectionTask = &createCollectionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, collectionTask.Type())
	ts, err = collectionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = collectionTask.Execute()
	assert.NotNil(t, err)
}

func TestMaster_DropCollectionTask(t *testing.T) {
	req := internalpb.DropCollectionRequest{
		MsgType:        internalpb.MsgType_kDropPartition,
		ReqID:          1,
		Timestamp:      11,
		ProxyID:        1,
		CollectionName: nil,
	}
	var collectionTask task = &dropCollectionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kDropPartition, collectionTask.Type())
	ts, err := collectionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	collectionTask = &dropCollectionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, collectionTask.Type())
	ts, err = collectionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = collectionTask.Execute()
	assert.NotNil(t, err)
}

func TestMaster_HasCollectionTask(t *testing.T) {
	req := internalpb.HasCollectionRequest{
		MsgType:        internalpb.MsgType_kHasCollection,
		ReqID:          1,
		Timestamp:      11,
		ProxyID:        1,
		CollectionName: nil,
	}
	var collectionTask task = &hasCollectionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kHasCollection, collectionTask.Type())
	ts, err := collectionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	collectionTask = &hasCollectionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, collectionTask.Type())
	ts, err = collectionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = collectionTask.Execute()
	assert.NotNil(t, err)
}

func TestMaster_ShowCollectionTask(t *testing.T) {
	req := internalpb.ShowCollectionRequest{
		MsgType:   internalpb.MsgType_kShowCollections,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
	}
	var collectionTask task = &showCollectionsTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kShowCollections, collectionTask.Type())
	ts, err := collectionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	collectionTask = &showCollectionsTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, collectionTask.Type())
	ts, err = collectionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = collectionTask.Execute()
	assert.NotNil(t, err)
}

func TestMaster_DescribeCollectionTask(t *testing.T) {
	req := internalpb.DescribeCollectionRequest{
		MsgType:        internalpb.MsgType_kDescribeCollection,
		ReqID:          1,
		Timestamp:      11,
		ProxyID:        1,
		CollectionName: nil,
	}
	var collectionTask task = &describeCollectionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kDescribeCollection, collectionTask.Type())
	ts, err := collectionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	collectionTask = &describeCollectionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, collectionTask.Type())
	ts, err = collectionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = collectionTask.Execute()
	assert.NotNil(t, err)
}

func TestMaster_CreatePartitionTask(t *testing.T) {
	req := internalpb.CreatePartitionRequest{
		MsgType:       internalpb.MsgType_kCreatePartition,
		ReqID:         1,
		Timestamp:     11,
		ProxyID:       1,
		PartitionName: nil,
	}
	var partitionTask task = &createPartitionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kCreatePartition, partitionTask.Type())
	ts, err := partitionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	partitionTask = &createPartitionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, partitionTask.Type())
	ts, err = partitionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = partitionTask.Execute()
	assert.NotNil(t, err)
}
func TestMaster_DropPartitionTask(t *testing.T) {
	req := internalpb.DropPartitionRequest{
		MsgType:       internalpb.MsgType_kDropPartition,
		ReqID:         1,
		Timestamp:     11,
		ProxyID:       1,
		PartitionName: nil,
	}
	var partitionTask task = &dropPartitionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kDropPartition, partitionTask.Type())
	ts, err := partitionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	partitionTask = &dropPartitionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, partitionTask.Type())
	ts, err = partitionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = partitionTask.Execute()
	assert.NotNil(t, err)
}
func TestMaster_HasPartitionTask(t *testing.T) {
	req := internalpb.HasPartitionRequest{
		MsgType:       internalpb.MsgType_kHasPartition,
		ReqID:         1,
		Timestamp:     11,
		ProxyID:       1,
		PartitionName: nil,
	}
	var partitionTask task = &hasPartitionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kHasPartition, partitionTask.Type())
	ts, err := partitionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	partitionTask = &hasPartitionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, partitionTask.Type())
	ts, err = partitionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = partitionTask.Execute()
	assert.NotNil(t, err)
}
func TestMaster_DescribePartitionTask(t *testing.T) {
	req := internalpb.DescribePartitionRequest{
		MsgType:       internalpb.MsgType_kDescribePartition,
		ReqID:         1,
		Timestamp:     11,
		ProxyID:       1,
		PartitionName: nil,
	}
	var partitionTask task = &describePartitionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kDescribePartition, partitionTask.Type())
	ts, err := partitionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	partitionTask = &describePartitionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, partitionTask.Type())
	ts, err = partitionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = partitionTask.Execute()
	assert.NotNil(t, err)
}
func TestMaster_ShowPartitionTask(t *testing.T) {
	req := internalpb.ShowPartitionRequest{
		MsgType:   internalpb.MsgType_kShowPartitions,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
	}
	var partitionTask task = &showPartitionTask{
		req:      &req,
		baseTask: baseTask{},
	}
	assert.Equal(t, internalpb.MsgType_kShowPartitions, partitionTask.Type())
	ts, err := partitionTask.Ts()
	assert.Equal(t, uint64(11), ts)
	assert.Nil(t, err)

	partitionTask = &showPartitionTask{
		req:      nil,
		baseTask: baseTask{},
	}

	assert.Equal(t, internalpb.MsgType_kNone, partitionTask.Type())
	ts, err = partitionTask.Ts()
	assert.Equal(t, uint64(0), ts)
	assert.NotNil(t, err)
	err = partitionTask.Execute()
	assert.NotNil(t, err)
}
