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

package proxy

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var _ types.QueryNode = &QueryNodeMock{}

type QueryNodeMock struct {
	nodeID  typeutil.UniqueID
	address string

	state atomic.Value // internal.StateCode

	withStatisticsResponse *internalpb.GetStatisticsResponse
	withSearchResult       *internalpb.SearchResults
	withQueryResult        *internalpb.RetrieveResults
	queryError             error
	searchError            error
	statisticsError        error
}

func (m *QueryNodeMock) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	if m.statisticsError != nil {
		return nil, m.statisticsError
	}
	return m.withStatisticsResponse, nil
}

func (m *QueryNodeMock) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	if m.searchError != nil {
		return nil, m.searchError
	}
	return m.withSearchResult, nil
}

func (m *QueryNodeMock) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	if m.queryError != nil {
		return nil, m.queryError
	}
	return m.withQueryResult, nil
}

func (m *QueryNodeMock) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

// TODO
func (m *QueryNodeMock) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return nil, nil
}

// TODO
func (m *QueryNodeMock) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	return nil, nil
}

// TODO
func (m *QueryNodeMock) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

// TODO
func (m *QueryNodeMock) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

// TODO
func (m *QueryNodeMock) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	return nil, nil
}

// TODO
func (m *QueryNodeMock) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return nil, nil
}

// TODO
func (m *QueryNodeMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *QueryNodeMock) Init() error     { return nil }
func (m *QueryNodeMock) Start() error    { return nil }
func (m *QueryNodeMock) Stop() error     { return nil }
func (m *QueryNodeMock) Register() error { return nil }
func (m *QueryNodeMock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return nil, nil
}
func (m *QueryNodeMock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}
func (m *QueryNodeMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}
func (m *QueryNodeMock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return nil, nil
}
