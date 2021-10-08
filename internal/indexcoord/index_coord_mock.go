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
	"errors"
	"strconv"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Mock is an alternative to IndexCoord, it will return specific results based on specific parameters.
type Mock struct {
	etcdKV *etcdkv.EtcdKV

	Failure bool
}

// Init initializes the Mock of IndexCoord. When param `Failure` is true, it will return an error.
func (icm *Mock) Init() error {
	if icm.Failure {
		return errors.New("IndexCoordinate init failed")
	}
	return nil
}

// Start starts the Mock of IndexCoord. When param `Failure` is true, it will return an error.
func (icm *Mock) Start() error {
	if icm.Failure {
		return errors.New("IndexCoordinate start failed")
	}
	return nil
}

// Stop stops the Mock of IndexCoord. When param `Failure` is true, it will return an error.
func (icm *Mock) Stop() error {
	if icm.Failure {
		return errors.New("IndexCoordinate stop failed")
	}
	err := icm.etcdKV.RemoveWithPrefix("session/" + typeutil.IndexCoordRole)
	return err
}

// Register registers an IndexCoord role in ETCD, if Param `Failure` is true, it will return an error.
func (icm *Mock) Register() error {
	if icm.Failure {
		return errors.New("IndexCoordinate register failed")
	}
	icm.etcdKV, _ = etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	err := icm.etcdKV.RemoveWithPrefix("session/" + typeutil.IndexCoordRole)
	session := sessionutil.NewSession(context.Background(), Params.MetaRootPath, Params.EtcdEndpoints)
	session.Init(typeutil.IndexCoordRole, Params.Address, true)
	return err
}

// GetComponentStates gets the component states of the mocked IndexCoord, if Param `Failure` is true, it will return an error,
// and the state is `StateCode_Abnormal`. Under normal circumstances the state is `StateCode_Healthy`.
func (icm *Mock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	if icm.Failure {
		return &internalpb.ComponentStates{
			State: &internalpb.ComponentInfo{
				StateCode: internalpb.StateCode_Abnormal,
			},
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexCoordinate GetComponentStates failed")
	}
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			StateCode: internalpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetStatisticsChannel gets the statistics channel of the mocked IndexCoord, if Param `Failure` is true, it will return an error.
func (icm *Mock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if icm.Failure {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexCoordinate GetStatisticsChannel failed")
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: "",
	}, nil
}

// GetTimeTickChannel gets the time tick channel of the mocked IndexCoord, if Param `Failure` is true, it will return an error.
func (icm *Mock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if icm.Failure {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexCoordinate GetTimeTickChannel failed")
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: "",
	}, nil
}

// BuildIndex receives a building index request, and return success, if Param `Failure` is true, it will return an error.
func (icm *Mock) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	if icm.Failure {
		return &indexpb.BuildIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
			IndexBuildID: 0,
		}, errors.New("IndexCoordinate BuildIndex error")
	}
	return &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexBuildID: 0,
	}, nil
}

// DropIndex receives a dropping index request, and return success, if Param `Failure` is true, it will return an error.
func (icm *Mock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	if icm.Failure {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, errors.New("IndexCoordinate DropIndex failed")
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// GetIndexStates gets the indexes states, if Param `Failure` is true, it will return an error.
// Under normal circumstances the state of each index is `IndexState_Finished`.
func (icm *Mock) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	if icm.Failure {
		return &indexpb.GetIndexStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexCoordinate GetIndexStates failed")
	}
	states := make([]*indexpb.IndexInfo, len(req.IndexBuildIDs))
	for i := range states {
		states[i] = &indexpb.IndexInfo{
			IndexBuildID: req.IndexBuildIDs[i],
			State:        commonpb.IndexState_Finished,
			IndexID:      0,
		}
	}
	return &indexpb.GetIndexStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		States: states,
	}, nil
}

// GetIndexFilePaths gets the index file paths, if Param `Failure` is true, it will return an error.
func (icm *Mock) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	if icm.Failure {
		return &indexpb.GetIndexFilePathsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexCoordinate GetIndexFilePaths failed")
	}
	filePaths := make([]*indexpb.IndexFilePathInfo, len(req.IndexBuildIDs))
	for i := range filePaths {
		filePaths[i] = &indexpb.IndexFilePathInfo{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			IndexBuildID:   req.IndexBuildIDs[i],
			IndexFilePaths: []string{strconv.FormatInt(req.IndexBuildIDs[i], 10)},
		}
	}
	return &indexpb.GetIndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		FilePaths: filePaths,
	}, nil
}

// GetMetrics gets the metrics of mocked IndexCoord, if Param `Failure` is true, it will return an error.
func (icm *Mock) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if icm.Failure {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexCoordinate GetMetrics failed")
	}
	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Response:      "",
		ComponentName: "IndexCoord",
	}, nil
}
