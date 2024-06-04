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

package server

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/lognode/server/flush/pipeline"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// getQuotaMetrics returns DataNodeQuotaMetrics.
func (node *LogNode) getQuotaMetrics() (*metricsinfo.DataNodeQuotaMetrics, error) {
	var err error
	rms := make([]metricsinfo.RateMetric, 0)
	getRateMetric := func(label metricsinfo.RateMetricLabel) {
		rate, err2 := pipeline.RateCol.Rate(label, ratelimitutil.DefaultAvgDuration)
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

	minFGChannel, minFGTt := pipeline.RateCol.GetMinFlowGraphTt()

	var collectionSet []int64
	node.pipelineManager.Foreach(func(p *pipeline.Pipeline) {
		collectionSet = append(collectionSet, p.GetMetaCache().Collection())
	})

	return &metricsinfo.DataNodeQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
		Fgm: metricsinfo.FlowGraphMetric{
			MinFlowGraphChannel: minFGChannel,
			MinFlowGraphTt:      minFGTt,
			NumFlowGraph:        node.pipelineManager.Count(),
		},

		Effect: metricsinfo.NodeEffect{
			NodeID:        node.session.ServerID,
			CollectionIDs: collectionSet,
		},
	}, nil
}

func (node *LogNode) getSystemInfoMetrics(_ context.Context, _ *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()

	quotaMetrics, err := node.getQuotaMetrics()
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
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
			ID:            node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.DataNodeConfiguration{
			FlushInsertBufferSize: paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64(),
		},
		QuotaMetrics: quotaMetrics,
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, paramtable.GetNodeID()),
	}, nil
}
