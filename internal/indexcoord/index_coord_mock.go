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

package indexcoord

import (
	"context"
	"errors"
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Mock is an alternative to IndexCoord, it will return specific results based on specific parameters.
type Mock struct {
	types.IndexCoord

	CallInit                 func() error
	CallStart                func() error
	CallStop                 func() error
	CallGetComponentStates   func(ctx context.Context) (*internalpb.ComponentStates, error)
	CallGetStatisticsChannel func(ctx context.Context) (*milvuspb.StringResponse, error)
	CallRegister             func() error

	CallSetEtcdClient   func(etcdClient *clientv3.Client)
	CallSetDataCoord    func(dataCoord types.DataCoord) error
	CallUpdateStateCode func(stateCode internalpb.StateCode)

	CallCreateIndex           func(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error)
	CallGetIndexState         func(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error)
	CallGetSegmentIndexState  func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error)
	CallGetIndexInfos         func(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error)
	CallDescribeIndex         func(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error)
	CallGetIndexBuildProgress func(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error)
	CallDropIndex             func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
	CallGetMetrics            func(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// Init initializes the Mock of IndexCoord. When param `Failure` is true, it will return an error.
func (m *Mock) Init() error {
	return m.CallInit()
}

// Start starts the Mock of IndexCoord. When param `Failure` is true, it will return an error.
func (m *Mock) Start() error {
	return m.CallStart()
}

// Stop stops the Mock of IndexCoord. When param `Failure` is true, it will return an error.
func (m *Mock) Stop() error {
	return m.CallStop()
}

// Register registers an IndexCoord role in ETCD, if Param `Failure` is true, it will return an error.
func (m *Mock) Register() error {
	return m.CallRegister()
}

func (m *Mock) SetEtcdClient(client *clientv3.Client) {
	m.CallSetEtcdClient(client)
}

func (m *Mock) SetDataCoord(dataCoord types.DataCoord) error {
	return m.CallSetDataCoord(dataCoord)
}

func (m *Mock) UpdateStateCode(stateCode internalpb.StateCode) {
	m.CallUpdateStateCode(stateCode)
}

// GetComponentStates gets the component states of the mocked IndexCoord.
func (m *Mock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return m.CallGetComponentStates(ctx)
}

// GetStatisticsChannel gets the statistics channel of the mocked IndexCoord, if Param `Failure` is true, it will return an error.
func (m *Mock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return m.CallGetStatisticsChannel(ctx)
}

func (m *Mock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return m.CallCreateIndex(ctx, req)
}

func (m *Mock) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return m.CallGetIndexState(ctx, req)
}

func (m *Mock) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return m.CallGetSegmentIndexState(ctx, req)
}

func (m *Mock) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return m.CallGetIndexInfos(ctx, req)
}

func (m *Mock) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return m.CallDescribeIndex(ctx, req)
}

func (m *Mock) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return m.CallGetIndexBuildProgress(ctx, req)
}

func (m *Mock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return m.CallDropIndex(ctx, req)
}

func (m *Mock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return m.CallGetMetrics(ctx, req)
}

func NewIndexCoordMock() *Mock {
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
		CallSetDataCoord: func(dataCoord types.DataCoord) error {
			return nil
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
		CallCreateIndex: func(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil
		},
		CallGetIndexState: func(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
			return &indexpb.GetIndexStateResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				State: commonpb.IndexState_Finished,
			}, nil
		},
		CallGetSegmentIndexState: func(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
			segmentStates := make([]*indexpb.SegmentIndexState, 0)
			for _, segID := range req.SegmentIDs {
				segmentStates = append(segmentStates, &indexpb.SegmentIndexState{
					SegmentID: segID,
					State:     commonpb.IndexState_Finished,
				})
			}
			return &indexpb.GetSegmentIndexStateResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				States: segmentStates,
			}, nil
		},
		CallGetIndexInfos: func(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
			filePaths := make([]*indexpb.IndexFilePathInfo, 0)
			for _, segID := range req.SegmentIDs {
				filePaths = append(filePaths, &indexpb.IndexFilePathInfo{
					SegmentID:      segID,
					IndexName:      "default",
					IndexFilePaths: []string{"file1", "file2"},
				})
			}
			return &indexpb.GetIndexInfoResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				EnableIndex: true,
				FilePaths:   filePaths,
			}, nil
		},
		CallDescribeIndex: func(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				IndexInfos: []*indexpb.IndexInfo{
					{
						CollectionID: 1,
						FieldID:      0,
						IndexName:    "default",
						IndexID:      0,
						TypeParams:   nil,
						IndexParams:  nil,
					},
				},
			}, nil
		},
		CallGetIndexBuildProgress: func(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
			return &indexpb.GetIndexBuildProgressResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				IndexedRows: 10240,
				TotalRows:   10240,
			}, nil
		},
		CallDropIndex: func(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil
		},
		CallGetMetrics: func(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				ComponentName: typeutil.IndexCoordRole,
			}, nil
		},
	}
}

type DataCoordMock struct {
	types.DataCoord

	CallGetSegmentInfo     func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error)
	CallGetFlushedSegment  func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error)
	CallAcquireSegmentLock func(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error)
	CallReleaseSegmentLock func(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error)
}

func (dcm *DataCoordMock) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return dcm.CallGetSegmentInfo(ctx, req)
}
func (dcm *DataCoordMock) AcquireSegmentLock(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
	return dcm.CallAcquireSegmentLock(ctx, req)
}

func (dcm *DataCoordMock) ReleaseSegmentLock(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
	return dcm.CallReleaseSegmentLock(ctx, req)
}

func (dcm *DataCoordMock) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return dcm.CallGetFlushedSegment(ctx, req)
}

// ChunkManagerMock is mock
// deprecated
type ChunkManagerMock struct {
	storage.ChunkManager

	Fail bool
	Err  bool
}

func (cmm *ChunkManagerMock) Exist(path string) (bool, error) {
	if cmm.Err {
		return false, errors.New("path not exist")
	}
	if cmm.Fail {
		return false, nil
	}
	return true, nil
}

func (cmm *ChunkManagerMock) RemoveWithPrefix(prefix string) error {
	if cmm.Err {
		return errors.New("error occurred")
	}
	if cmm.Fail {
		return nil
	}
	return nil
}

type mockETCDKV struct {
	kv.MetaKv

	save                        func(string, string) error
	remove                      func(string) error
	watchWithRevision           func(string, int64) clientv3.WatchChan
	loadWithRevisionAndVersions func(string) ([]string, []string, []int64, int64, error)
	compareVersionAndSwap       func(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error)
	loadWithPrefix2             func(key string) ([]string, []string, []int64, error)
}

func (mk *mockETCDKV) Save(key string, value string) error {
	return mk.save(key, value)
}

func (mk *mockETCDKV) Remove(key string) error {
	return mk.remove(key)
}

func (mk *mockETCDKV) LoadWithRevisionAndVersions(prefix string) ([]string, []string, []int64, int64, error) {
	return mk.loadWithRevisionAndVersions(prefix)
}

func (mk *mockETCDKV) CompareVersionAndSwap(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
	return mk.compareVersionAndSwap(key, version, target, opts...)
}

func (mk *mockETCDKV) LoadWithPrefix2(key string) ([]string, []string, []int64, error) {
	return mk.loadWithPrefix2(key)
}

func (mk *mockETCDKV) WatchWithRevision(key string, revision int64) clientv3.WatchChan {
	return mk.watchWithRevision(key, revision)
}

type chunkManagerMock struct {
	storage.ChunkManager

	removeWithPrefix func(string) error
	listWithPrefix   func(string, bool) ([]string, []time.Time, error)
	remove           func(string) error
}

func (cmm *chunkManagerMock) RemoveWithPrefix(prefix string) error {
	return cmm.removeWithPrefix(prefix)
}

func (cmm *chunkManagerMock) ListWithPrefix(prefix string, recursive bool) ([]string, []time.Time, error) {
	return cmm.listWithPrefix(prefix, recursive)
}

func (cmm *chunkManagerMock) Remove(key string) error {
	return cmm.remove(key)
}
