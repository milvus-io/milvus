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

package datanode

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// getQuotaMetrics returns DataNodeQuotaMetrics.
func (node *DataNode) getQuotaMetrics() (*metricsinfo.DataNodeQuotaMetrics, error) {
	var err error
	rms := make([]metricsinfo.RateMetric, 0)
	getRateMetric := func(label metricsinfo.RateMetricLabel) {
		rate, err2 := rateCol.Rate(label, ratelimitutil.DefaultAvgDuration)
		if err2 != nil {
			err = err2
			return
		}
		rms = append(rms, metricsinfo.RateMetric{
			Label: label,
			Rate:  rate,
		})
	}
	getRateMetric(metricsinfo.InsertConsumeThroughput)
	getRateMetric(metricsinfo.DeleteConsumeThroughput)
	if err != nil {
		return nil, err
	}

	getAllCollections := func() []int64 {
		collectionSet := typeutil.UniqueSet{}
		node.flowgraphManager.flowgraphs.Range(func(key, value any) bool {
			fg := value.(*dataSyncService)
			collectionSet.Insert(fg.channel.getCollectionID())
			return true
		})

		return collectionSet.Collect()
	}
	minFGChannel, minFGTt := rateCol.getMinFlowGraphTt()
	return &metricsinfo.DataNodeQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
		Fgm: metricsinfo.FlowGraphMetric{
			MinFlowGraphChannel: minFGChannel,
			MinFlowGraphTt:      minFGTt,
			NumFlowGraph:        node.flowgraphManager.getFlowGraphNum(),
		},
		Effect: metricsinfo.NodeEffect{
			NodeID:        node.GetSession().ServerID,
			CollectionIDs: getAllCollections(),
		},
	}, nil
}

func (node *DataNode) getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()

	quotaMetrics, err := node.getQuotaMetrics()
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
		}, nil
	}
	hardwareMetrics := metricsinfo.HardwareMetrics{
		IP:           node.session.Address,
		CPUCoreCount: hardware.GetCPUNum(),
		CPUCoreUsage: hardware.GetCPUUsage(),
		Memory:       totalMem,
		MemoryUsage:  usedMem,
		Disk:         hardware.GetDiskCount(),
		DiskUsage:    hardware.GetDiskUsage(),
	}
	quotaMetrics.Hms = hardwareMetrics

	nodeInfos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name:          metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
			HardwareInfos: hardwareMetrics,
			SystemInfo:    metricsinfo.DeployMetrics{},
			CreatedTime:   paramtable.GetCreateTime().String(),
			UpdatedTime:   paramtable.GetUpdateTime().String(),
			Type:          typeutil.DataNodeRole,
			ID:            node.GetSession().ServerID,
		},
		SystemConfigurations: metricsinfo.DataNodeConfiguration{
			FlushInsertBufferSize: Params.DataNodeCfg.FlushInsertBufferSize.GetAsInt64(),
		},
		QuotaMetrics: quotaMetrics,
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
	}, nil
}
