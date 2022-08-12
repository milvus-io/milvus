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

package indexnode

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Mock is an alternative to IndexNode, it will return specific results based on specific parameters.
type Mock struct {
	types.IndexNode

	CallInit                 func() error
	CallStart                func() error
	CallStop                 func() error
	CallGetComponentStates   func(ctx context.Context) (*internalpb.ComponentStates, error)
	CallGetStatisticsChannel func(ctx context.Context) (*milvuspb.StringResponse, error)
	CallRegister             func() error

	CallSetEtcdClient   func(etcdClient *clientv3.Client)
	CallUpdateStateCode func(stateCode internalpb.StateCode)

	CallCreateJob func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error)
	CallQueryJobs func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error)
	CallDropJobs  func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error)
	CallGetJobNum func(ctx context.Context, in *indexpb.GetJobNumRequest) (*indexpb.GetJobNumResponse, error)

	CallGetMetrics         func(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	CallShowConfigurations func(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
}

func NewIndexNodeMock() *Mock {
	return &Mock{
		CallInit: func() error {
			return nil
		},
		CallStart: func() error {
			return nil
		},
		CallRegister: func() error {
			return nil
		},
		CallStop: func() error {
			return nil
		},
		CallSetEtcdClient: func(etcdClient *clientv3.Client) {
			return
		},
		CallUpdateStateCode: func(stateCode internalpb.StateCode) {
			return
		},
		CallGetComponentStates: func(ctx context.Context) (*internalpb.ComponentStates, error) {
			return &internalpb.ComponentStates{
				State: &internalpb.ComponentInfo{
					NodeID:    1,
					Role:      typeutil.IndexCoordRole,
					StateCode: internalpb.StateCode_Healthy,
				},
				SubcomponentStates: nil,
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, nil
		},
		CallGetStatisticsChannel: func(ctx context.Context) (*milvuspb.StringResponse, error) {
			return &milvuspb.StringResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, nil
		},
		CallCreateJob: func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil
		},
		CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
			indexInfos := make([]*indexpb.IndexTaskInfo, 0)
			for _, buildID := range in.BuildIDs {
				indexInfos = append(indexInfos, &indexpb.IndexTaskInfo{
					BuildID:    buildID,
					State:      commonpb.IndexState_Finished,
					IndexFiles: []string{"file1", "file2"},
				})
			}
			return &indexpb.QueryJobsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				ClusterID:  in.ClusterID,
				IndexInfos: indexInfos,
			}, nil
		},
		CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil
		},
		CallGetJobNum: func(ctx context.Context, in *indexpb.GetJobNumRequest) (*indexpb.GetJobNumResponse, error) {
			return &indexpb.GetJobNumResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				TotalJobNum:      1,
				EnqueueJobNum:    0,
				InProgressJobNum: 1,
				TaskSlots:        1,
				JobInfos: []*indexpb.JobInfo{
					{
						NumRows:   1024,
						Dim:       128,
						StartTime: 1,
						EndTime:   10,
						PodID:     1,
					},
				},
			}, nil
		},
		CallGetMetrics: func(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
			return getMockSystemInfoMetrics(ctx, req, nil)
		},
	}
}

func (m *Mock) Init() error {
	return m.CallInit()
}

func (m *Mock) Start() error {
	return m.CallStart()
}

func (m *Mock) Stop() error {
	return m.CallStop()
}

func (m *Mock) Register() error {
	return m.CallRegister()
}

func (m *Mock) CreateJob(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
	return m.CallCreateJob(ctx, req)
}

func (m *Mock) QueryJobs(ctx context.Context, req *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
	return m.CallQueryJobs(ctx, req)
}

func (m *Mock) DropJobs(ctx context.Context, req *indexpb.DropJobsRequest) (*commonpb.Status, error) {
	return m.CallDropJobs(ctx, req)
}

func (m *Mock) GetJobNum(ctx context.Context, req *indexpb.GetJobNumRequest) (*indexpb.GetJobNumResponse, error) {
	return m.CallGetJobNum(ctx, req)
}

func (m *Mock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.CallGetMetrics(ctx, req)
}

//ShowConfigurations returns the configurations of Mock indexNode matching req.Pattern
func (m *Mock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return m.CallShowConfigurations(ctx, req)
}

func getMockSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
	node *Mock,
) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics
	nodeInfos := metricsinfo.IndexNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, Params.IndexNodeCfg.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       1000,
				MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: Params.IndexNodeCfg.CreatedTime.String(),
			UpdatedTime: Params.IndexNodeCfg.UpdatedTime.String(),
			Type:        typeutil.IndexNodeRole,
		},
		SystemConfigurations: metricsinfo.IndexNodeConfiguration{
			MinioBucketName: Params.MinioCfg.BucketName,
			SimdType:        Params.CommonCfg.SimdType,
		},
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, _ := metricsinfo.MarshalComponentInfos(nodeInfos)

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, Params.IndexNodeCfg.GetNodeID()),
	}, nil
}
