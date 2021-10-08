// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestGetSystemInfoMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.MetaRootPath, Params.EtcdEndpoints)

	req := &milvuspb.GetMetricsRequest{
		Base: genCommonMsgBase(commonpb.MsgType_WatchQueryChannels),
	}
	resp, err := getSystemInfoMetrics(ctx, req, node)
	assert.NoError(t, err)
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
}

func TestCheckSegmentMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	genSegmentLoadInfo := func() *querypb.SegmentLoadInfo {
		return &querypb.SegmentLoadInfo{
			SegmentID:    defaultSegmentID,
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
			NumOfRows:    1,
		}
	}

	t.Run("valid test", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		err = checkSegmentMemory([]*querypb.SegmentLoadInfo{genSegmentLoadInfo()}, node.historical.replica, node.streaming.replica)
		assert.NoError(t, err)
	})

	t.Run("test no collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.historical.replica.freeAll()
		err = checkSegmentMemory([]*querypb.SegmentLoadInfo{genSegmentLoadInfo()}, node.historical.replica, node.streaming.replica)
		assert.Error(t, err)
	})

	t.Run("test OOM", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		totalRAM := Params.CacheSize * 1024 * 1024 * 1024

		col, err := node.historical.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		assert.NoError(t, err)

		info := genSegmentLoadInfo()
		info.NumOfRows = totalRAM / int64(sizePerRecord)
		err = checkSegmentMemory([]*querypb.SegmentLoadInfo{info}, node.historical.replica, node.streaming.replica)
		assert.Error(t, err)
	})

	t.Run("test EstimateSizePerRecord failed", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		col, err := node.historical.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		for _, param := range col.schema.Fields[0].TypeParams {
			if param.Key == "dim" {
				param.Value = "%&^%&"
			}
		}

		info := genSegmentLoadInfo()
		err = checkSegmentMemory([]*querypb.SegmentLoadInfo{info}, node.historical.replica, node.streaming.replica)
		assert.Error(t, err)
	})
}
