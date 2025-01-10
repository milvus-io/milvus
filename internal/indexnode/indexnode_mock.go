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
	"fmt"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Mock is an alternative to IndexNode, it will return specific results based on specific parameters.
type Mock struct {
	types.IndexNode

	CallInit                 func() error
	CallStart                func() error
	CallStop                 func() error
	CallGetComponentStates   func(ctx context.Context) (*milvuspb.ComponentStates, error)
	CallGetStatisticsChannel func(ctx context.Context) (*milvuspb.StringResponse, error)
	CallRegister             func() error

	CallSetAddress      func(address string)
	CallSetEtcdClient   func(etcdClient *clientv3.Client)
	CallUpdateStateCode func(stateCode commonpb.StateCode)

	CallCreateJob   func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error)
	CallQueryJobs   func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error)
	CallDropJobs    func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error)
	CallGetJobStats func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error)
	CallCreateJobV2 func(ctx context.Context, req *indexpb.CreateJobV2Request) (*commonpb.Status, error)
	CallQueryJobV2  func(ctx context.Context, req *indexpb.QueryJobsV2Request) (*indexpb.QueryJobsV2Response, error)
	CallDropJobV2   func(ctx context.Context, req *indexpb.DropJobsV2Request) (*commonpb.Status, error)

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
		CallSetAddress: func(address string) {
		},
		CallSetEtcdClient: func(etcdClient *clientv3.Client) {
		},
		CallUpdateStateCode: func(stateCode commonpb.StateCode) {
		},
		CallGetComponentStates: func(ctx context.Context) (*milvuspb.ComponentStates, error) {
			return &milvuspb.ComponentStates{
				State: &milvuspb.ComponentInfo{
					NodeID:    1,
					Role:      typeutil.IndexNodeRole,
					StateCode: commonpb.StateCode_Healthy,
				},
				SubcomponentStates: nil,
				Status:             merr.Success(),
			}, nil
		},
		CallGetStatisticsChannel: func(ctx context.Context) (*milvuspb.StringResponse, error) {
			return &milvuspb.StringResponse{
				Status: merr.Success(),
			}, nil
		},
		CallCreateJob: func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		},
		CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
			indexInfos := make([]*indexpb.IndexTaskInfo, 0)
			for _, buildID := range in.BuildIDs {
				indexInfos = append(indexInfos, &indexpb.IndexTaskInfo{
					BuildID:       buildID,
					State:         commonpb.IndexState_Finished,
					IndexFileKeys: []string{"file1", "file2"},
				})
			}
			return &indexpb.QueryJobsResponse{
				Status:     merr.Success(),
				ClusterID:  in.ClusterID,
				IndexInfos: indexInfos,
			}, nil
		},
		CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		},
		CallCreateJobV2: func(ctx context.Context, req *indexpb.CreateJobV2Request) (*commonpb.Status, error) {
			return merr.Success(), nil
		},
		CallQueryJobV2: func(ctx context.Context, req *indexpb.QueryJobsV2Request) (*indexpb.QueryJobsV2Response, error) {
			switch req.GetJobType() {
			case indexpb.JobType_JobTypeIndexJob:
				results := make([]*indexpb.IndexTaskInfo, 0)
				for _, buildID := range req.GetTaskIDs() {
					results = append(results, &indexpb.IndexTaskInfo{
						BuildID:             buildID,
						State:               commonpb.IndexState_Finished,
						IndexFileKeys:       []string{},
						SerializedSize:      1024,
						FailReason:          "",
						CurrentIndexVersion: 1,
						IndexStoreVersion:   1,
					})
				}
				return &indexpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: req.GetClusterID(),
					Result: &indexpb.QueryJobsV2Response_IndexJobResults{
						IndexJobResults: &indexpb.IndexJobResults{
							Results: results,
						},
					},
				}, nil
			case indexpb.JobType_JobTypeAnalyzeJob:
				results := make([]*indexpb.AnalyzeResult, 0)
				for _, taskID := range req.GetTaskIDs() {
					results = append(results, &indexpb.AnalyzeResult{
						TaskID:        taskID,
						State:         indexpb.JobState_JobStateFinished,
						CentroidsFile: fmt.Sprintf("%d/stats_file", taskID),
						FailReason:    "",
					})
				}
				return &indexpb.QueryJobsV2Response{
					Status:    merr.Success(),
					ClusterID: req.GetClusterID(),
					Result: &indexpb.QueryJobsV2Response_AnalyzeJobResults{
						AnalyzeJobResults: &indexpb.AnalyzeResults{
							Results: results,
						},
					},
				}, nil
			default:
				return &indexpb.QueryJobsV2Response{
					Status:    merr.Status(errors.New("unknown job type")),
					ClusterID: req.GetClusterID(),
				}, nil
			}
		},
		CallDropJobV2: func(ctx context.Context, req *indexpb.DropJobsV2Request) (*commonpb.Status, error) {
			return merr.Success(), nil
		},
		CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
			return &indexpb.GetJobStatsResponse{
				Status:           merr.Success(),
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
		CallShowConfigurations: func(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
			return &internalpb.ShowConfigurationsResponse{
				Status: merr.Success(),
			}, nil
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

func (m *Mock) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return m.CallGetComponentStates(ctx)
}

func (m *Mock) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return m.CallGetStatisticsChannel(ctx)
}

func (m *Mock) Register() error {
	return m.CallRegister()
}

func (m *Mock) SetAddress(address string) {
	m.CallSetAddress(address)
}

func (m *Mock) GetAddress() string {
	return ""
}

func (m *Mock) SetEtcdClient(etcdClient *clientv3.Client) {
}

func (m *Mock) UpdateStateCode(stateCode commonpb.StateCode) {
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

func (m *Mock) GetJobStats(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
	return m.CallGetJobStats(ctx, req)
}

func (m *Mock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.CallGetMetrics(ctx, req)
}

func (m *Mock) CreateJobV2(ctx context.Context, req *indexpb.CreateJobV2Request) (*commonpb.Status, error) {
	return m.CallCreateJobV2(ctx, req)
}

func (m *Mock) QueryJobsV2(ctx context.Context, req *indexpb.QueryJobsV2Request) (*indexpb.QueryJobsV2Response, error) {
	return m.CallQueryJobV2(ctx, req)
}

func (m *Mock) DropJobsV2(ctx context.Context, req *indexpb.DropJobsV2Request) (*commonpb.Status, error) {
	return m.CallDropJobV2(ctx, req)
}

// ShowConfigurations returns the configurations of Mock indexNode matching req.Pattern
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
			Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, paramtable.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				CPUCoreCount: hardware.GetCPUNum(),
				CPUCoreUsage: hardware.GetCPUUsage(),
				Memory:       1000,
				MemoryUsage:  hardware.GetUsedMemoryCount(),
				Disk:         hardware.GetDiskCount(),
				DiskUsage:    hardware.GetDiskUsage(),
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: paramtable.GetCreateTime().String(),
			UpdatedTime: paramtable.GetUpdateTime().String(),
			Type:        typeutil.IndexNodeRole,
		},
		SystemConfigurations: metricsinfo.IndexNodeConfiguration{
			MinioBucketName: Params.MinioCfg.BucketName.GetValue(),
			SimdType:        Params.CommonCfg.SimdType.GetValue(),
		},
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, _ := metricsinfo.MarshalComponentInfos(nodeInfos)

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, paramtable.GetNodeID()),
	}, nil
}
