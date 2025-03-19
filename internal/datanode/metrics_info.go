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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// getQuotaMetrics returns DataNodeQuotaMetrics.
func (node *DataNode) getQuotaMetrics() (*metricsinfo.DataNodeQuotaMetrics, error) {
	var err error
	rms := make([]metricsinfo.RateMetric, 0)
	getRateMetric := func(label metricsinfo.RateMetricLabel) {
		rate, err2 := util.GetRateCollector().Rate(label, ratelimitutil.DefaultAvgDuration)
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

	minFGChannel, minFGTt := util.GetRateCollector().GetMinFlowGraphTt()
	return &metricsinfo.DataNodeQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
		Fgm: metricsinfo.FlowGraphMetric{
			MinFlowGraphChannel: minFGChannel,
			MinFlowGraphTt:      minFGTt,
			NumFlowGraph:        node.flowgraphManager.GetFlowgraphCount(),
		},
		Effect: metricsinfo.NodeEffect{
			NodeID:        node.GetSession().ServerID,
			CollectionIDs: node.flowgraphManager.GetCollectionIDs(),
		},
	}, nil
}

func (node *DataNode) getSystemInfoMetrics(ctx context.Context, _ *milvuspb.GetMetricsRequest) (string, error) {
	// TODO(dragondriver): add more metrics
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()

	quotaMetrics, err := node.getQuotaMetrics()
	if err != nil {
		return "", err
	}

	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
	}

	hardwareMetrics := metricsinfo.HardwareMetrics{
		IP:               node.session.Address,
		CPUCoreCount:     hardware.GetCPUNum(),
		CPUCoreUsage:     hardware.GetCPUUsage(),
		Memory:           totalMem,
		MemoryUsage:      usedMem,
		Disk:             total,
		DiskUsage:        used,
		IOWaitPercentage: ioWait,
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
			MinioBucketName:       Params.MinioCfg.BucketName.GetValue(),
			SimdType:              Params.CommonCfg.SimdType.GetValue(),
			FlushInsertBufferSize: Params.DataNodeCfg.FlushInsertBufferSize.GetAsInt64(),
		},
		QuotaMetrics: quotaMetrics,
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)
	return metricsinfo.MarshalComponentInfos(nodeInfos)
}
