// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestOutOfMemoryAndDisk(t *testing.T) {
	ctx := context.TODO()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	totalRAM := int64(hardware.GetMemoryCount())

	col, err := node.metaReplica.getCollectionByID(defaultCollectionID)
	assert.NoError(t, err)

	sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
	assert.NoError(t, err)

	binlogs := []*datapb.Binlog{
		{
			LogSize: totalRAM,
		},
	}

	indexPaths := []string{}
	_, indexParams := genIndexParams(IndexFaissIVFPQ, L2)
	indexInfo := &querypb.FieldIndexInfo{
		FieldID:        simpleFloatVecField.id,
		EnableIndex:    true,
		IndexName:      indexName,
		IndexID:        indexID,
		BuildID:        buildID,
		IndexParams:    funcutil.Map2KeyValuePair(indexParams),
		IndexFilePaths: indexPaths,
		IndexSize:      totalRAM,
	}

	segmentLoadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    defaultSegmentID,
		PartitionID:  defaultPartitionID,
		CollectionID: defaultCollectionID,
		NumOfRows:    totalRAM / int64(sizePerRecord),
		SegmentSize:  totalRAM,
		BinlogPaths:  []*datapb.FieldBinlog{{FieldID: simpleFloatVecField.id, Binlogs: binlogs}},
		IndexInfos:   []*querypb.FieldIndexInfo{indexInfo},
	}

	_, indexParams = genIndexParams(indexparamcheck.IndexDISKANN, L2)
	diskIndexInfo := &querypb.FieldIndexInfo{
		FieldID:        simpleFloatVecField.id,
		EnableIndex:    true,
		IndexName:      indexName,
		IndexID:        indexID,
		BuildID:        buildID,
		IndexParams:    funcutil.Map2KeyValuePair(indexParams),
		IndexFilePaths: indexPaths,
		IndexSize:      100 * 1024 * 1024,
	}
	diskSegmentLoadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    defaultSegmentID,
		PartitionID:  defaultPartitionID,
		CollectionID: defaultCollectionID,
		NumOfRows:    1,
		SegmentSize:  int64(sizePerRecord),
		BinlogPaths:  []*datapb.FieldBinlog{{FieldID: simpleFloatVecField.id, Binlogs: binlogs}},
		IndexInfos:   []*querypb.FieldIndexInfo{diskIndexInfo},
	}

	Params.QueryNodeCfg.DiskCapacityLimit = 0
	rm := ResourceManager{}
	rm.Init()

	// TEST OOM
	err = rm.reserve(defaultCollectionID, segmentLoadInfo)
	assert.Error(t, err)

	// TEST DISK
	err = rm.reserve(defaultCollectionID, diskSegmentLoadInfo)
	assert.Error(t, err)
}

func TestMemoryResourceAllocation(t *testing.T) {
	ctx := context.TODO()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	col, err := node.metaReplica.getCollectionByID(defaultCollectionID)
	assert.NoError(t, err)

	sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
	assert.NoError(t, err)

	binlogs := []*datapb.Binlog{
		{
			LogSize: 1,
		},
	}

	indexPaths := []string{}
	_, indexParams := genIndexParams(indexparamcheck.IndexDISKANN, L2)
	diskIndexInfo := &querypb.FieldIndexInfo{
		FieldID:        simpleFloatVecField.id,
		EnableIndex:    true,
		IndexName:      indexName,
		IndexID:        indexID,
		BuildID:        buildID,
		IndexParams:    funcutil.Map2KeyValuePair(indexParams),
		IndexFilePaths: indexPaths,
		IndexSize:      100 * 1024 * 1024,
	}
	diskSegmentLoadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    defaultSegmentID,
		PartitionID:  defaultPartitionID,
		CollectionID: defaultCollectionID,
		NumOfRows:    1,
		SegmentSize:  int64(sizePerRecord),
		BinlogPaths:  []*datapb.FieldBinlog{{FieldID: simpleFloatVecField.id, Binlogs: binlogs}},
		IndexInfos:   []*querypb.FieldIndexInfo{diskIndexInfo},
	}

	// use disk to test and make it more make sense
	Params.QueryNodeCfg.DiskCapacityLimit = 250 * 1024 * 1024
	rm := ResourceManager{}
	rm.Init()

	timer := time.NewTimer(10 * time.Millisecond)
	var n int32
	err = rm.reserve(defaultCollectionID, diskSegmentLoadInfo)
	assert.NoError(t, err)

	go func() {
		select {
		case <-timer.C:
			atomic.AddInt32(&n, 1)
			rm.release(defaultCollectionID, diskSegmentLoadInfo)
		case <-ctx.Done():
			return
		}
	}()

	err = rm.reserve(defaultCollectionID, diskSegmentLoadInfo)
	assert.NoError(t, err)

	// just can not reserve the third one
	err = rm.reserve(defaultCollectionID, diskSegmentLoadInfo)
	assert.NoError(t, err)

	// only works after release
	assert.Equal(t, atomic.LoadInt32(&n), int32(1))
}
