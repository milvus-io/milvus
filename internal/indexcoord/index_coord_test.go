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

package indexcoord

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"

	"github.com/milvus-io/milvus/internal/indexnode"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestIndexCoord(t *testing.T) {
	ctx := context.Background()
	inm0 := &indexnode.Mock{}
	err := inm0.Init()
	assert.Nil(t, err)
	err = inm0.Register()
	assert.Nil(t, err)
	err = inm0.Start()
	assert.Nil(t, err)
	ic, err := NewIndexCoord(ctx)
	assert.Nil(t, err)
	ic.reqTimeoutInterval = time.Second * 10
	ic.durationInterval = time.Second
	ic.assignTaskInterval = 200 * time.Millisecond
	ic.taskLimit = 20
	Params.Init()
	err = ic.Register()
	assert.Nil(t, err)

	err = ic.Init()
	assert.Nil(t, err)
	err = ic.Start()
	assert.Nil(t, err)

	err = inm0.Stop()
	assert.Nil(t, err)

	in, err := grpcindexnode.NewServer(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, in)
	inm := &indexnode.Mock{
		Build:   true,
		Failure: false,
	}
	err = in.SetClient(inm)
	assert.Nil(t, err)

	err = in.Run()
	assert.Nil(t, err)

	state, err := ic.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, internalpb.StateCode_Healthy, state.State.StateCode)

	indexID := int64(rand.Int())

	var indexBuildID UniqueID

	t.Run("Create Index", func(t *testing.T) {
		req := &indexpb.BuildIndexRequest{
			IndexID:   indexID,
			DataPaths: []string{"DataPath-1", "DataPath-2"},
		}
		resp, err := ic.BuildIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		indexBuildID = resp.IndexBuildID
		resp2, err := ic.BuildIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, indexBuildID, resp2.IndexBuildID)
		assert.Equal(t, "already have same index", resp2.Status.Reason)
	})

	t.Run("Get Index State", func(t *testing.T) {
		req := &indexpb.GetIndexStatesRequest{
			IndexBuildIDs: []UniqueID{indexBuildID},
		}
		for {
			resp, err := ic.GetIndexStates(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			if resp.States[0].State == commonpb.IndexState_Finished ||
				resp.States[0].State == commonpb.IndexState_Failed {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("Get IndexFile Paths", func(t *testing.T) {
		req := &indexpb.GetIndexFilePathsRequest{
			IndexBuildIDs: []UniqueID{indexBuildID},
		}
		resp, err := ic.GetIndexFilePaths(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.FilePaths))
		assert.Equal(t, 2, len(resp.FilePaths[0].IndexFilePaths))
		assert.Equal(t, "IndexFilePath-1", resp.FilePaths[0].IndexFilePaths[0])
		assert.Equal(t, "IndexFilePath-2", resp.FilePaths[0].IndexFilePaths[1])
	})

	t.Run("Drop Index", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			IndexID: indexID,
		}
		resp, err := ic.DropIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics, system info", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		log.Info("GetMetrics, system info",
			zap.String("name", resp.ComponentName),
			zap.String("resp", resp.Response))
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := ic.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := ic.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics when indexcoord is not healthy", func(t *testing.T) {
		ic.UpdateStateCode(internalpb.StateCode_Abnormal)
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		ic.UpdateStateCode(internalpb.StateCode_Healthy)
	})

	t.Run("GetMetrics when request is illegal", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType("GetIndexNodeMetrics")
		assert.Nil(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("Recycle IndexMeta", func(t *testing.T) {
		indexMeta := ic.metaTable.GetIndexMetaByIndexBuildID(indexBuildID)
		for indexMeta != nil {
			log.Info("RecycleIndexMeta", zap.Any("meta", indexMeta))
			indexMeta = ic.metaTable.GetIndexMetaByIndexBuildID(indexBuildID)
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("GetMetrics request without metricType", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "GetIndexCoordMetrics",
		}
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	err = in.Stop()
	assert.Nil(t, err)
	err = ic.Stop()
	assert.Nil(t, err)
}
