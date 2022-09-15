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

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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

	return &metricsinfo.DataNodeQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
		Fgm: metricsinfo.FlowGraphMetric{
			MinFlowGraphTt: rateCol.getMinFlowGraphTt(),
			NumFlowGraph:   node.flowgraphManager.getFlowGraphNum(),
		},
	}, nil
}

//getComponentConfigurations returns the configurations of dataNode matching req.Pattern
func getComponentConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) *internalpb.ShowConfigurationsResponse {
	prefix := "datanode."
	matchedConfig := Params.DataNodeCfg.Base.GetByPattern(prefix + req.Pattern)
	configList := make([]*commonpb.KeyValuePair, 0, len(matchedConfig))
	for key, value := range matchedConfig {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}
}

func (node *DataNode) getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics
	usedMem := metricsinfo.GetUsedMemoryCount()
	totalMem := metricsinfo.GetMemoryCount()

	quotaMetrics, err := node.getQuotaMetrics()
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
		}, nil
	}
	hardwareMetrics := metricsinfo.HardwareMetrics{
		IP:           node.session.Address,
		CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
		CPUCoreUsage: metricsinfo.GetCPUUsage(),
		Memory:       totalMem,
		MemoryUsage:  usedMem,
		Disk:         metricsinfo.GetDiskCount(),
		DiskUsage:    metricsinfo.GetDiskUsage(),
	}
	quotaMetrics.Hms = hardwareMetrics

	nodeInfos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name:          metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
			HardwareInfos: hardwareMetrics,
			SystemInfo:    metricsinfo.DeployMetrics{},
			CreatedTime:   Params.DataNodeCfg.CreatedTime.String(),
			UpdatedTime:   Params.DataNodeCfg.UpdatedTime.String(),
			Type:          typeutil.DataNodeRole,
			ID:            node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.DataNodeConfiguration{
			FlushInsertBufferSize: Params.DataNodeCfg.FlushInsertBufferSize,
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
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
	}, nil
}
