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

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type indexNodeMock struct {
	types.IndexNode
}

func (in *indexNodeMock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	indexMeta := indexpb.IndexMeta{}
	etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, err
	}
	_, values, versions, err := etcdKV.LoadWithPrefix2(req.MetaPath)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, err
	}
	err = proto.UnmarshalText(values[0], &indexMeta)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, err
	}
	indexMeta.IndexFilePaths = []string{"IndexFilePath-1", "IndexFilePath-2"}
	indexMeta.State = commonpb.IndexState_Finished
	_ = etcdKV.CompareVersionAndSwap(req.MetaPath, versions[0],
		proto.MarshalTextString(&indexMeta))

	time.Sleep(10 * time.Second)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func getSystemInfoMetricsByIndexNodeMock(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
	in *indexNodeMock,
) (*milvuspb.GetMetricsResponse, error) {

	id := UniqueID(16384)

	nodeInfos := metricsinfo.IndexNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, id),
		},
	}
	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, id),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, id),
	}, nil
}

func (in *indexNodeMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		return getSystemInfoMetricsByIndexNodeMock(ctx, req, in)
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

func TestIndexCoord(t *testing.T) {
	ctx := context.Background()
	ic, err := NewIndexCoord(ctx)
	assert.Nil(t, err)
	Params.Init()
	err = ic.Register()
	assert.Nil(t, err)
	// TODO: add indexNodeMock to etcd
	err = ic.Init()
	assert.Nil(t, err)
	indexNodeID := UniqueID(100)
	ic.nodeManager.setClient(indexNodeID, &indexNodeMock{})
	err = ic.Start()
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
			if resp.States[0].State == commonpb.IndexState_Finished {
				break
			}
			time.Sleep(3 * time.Second)
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

	time.Sleep(10 * time.Second)

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

	time.Sleep(11 * time.Second)
	ic.nodeManager.RemoveNode(indexNodeID)
	err = ic.Stop()
	assert.Nil(t, err)

}
