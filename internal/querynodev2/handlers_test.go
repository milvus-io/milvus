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

package querynodev2

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type HandlersSuite struct {
	suite.Suite
	// Data
	collectionID   int64
	collectionName string
	segmentID      int64
	channel        string

	// Dependency
	params              *paramtable.ComponentParam
	node                *QueryNode
	etcd                *clientv3.Client
	chunkManagerFactory *storage.ChunkManagerFactory

	// Mock
	factory *dependency.MockFactory
}

func (suite *HandlersSuite) SetupSuite() {
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.segmentID = 1
	suite.channel = "test-channel"
}

func (suite *HandlersSuite) SetupTest() {
	var err error
	paramtable.Init()
	suite.params = paramtable.Get()
	suite.params.Save(suite.params.CommonCfg.GCEnabled.Key, "false")

	// mock factory
	suite.factory = dependency.NewMockFactory(suite.T())
	suite.chunkManagerFactory = storage.NewChunkManagerFactory("local", objectstorage.RootPath(suite.T().TempDir()))

	// new node
	suite.node = NewQueryNode(context.Background(), suite.factory)
	// init etcd
	suite.etcd, err = etcd.GetEtcdClient(
		suite.params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		suite.params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		suite.params.EtcdCfg.Endpoints.GetAsStrings(),
		suite.params.EtcdCfg.EtcdTLSCert.GetValue(),
		suite.params.EtcdCfg.EtcdTLSKey.GetValue(),
		suite.params.EtcdCfg.EtcdTLSCACert.GetValue(),
		suite.params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	suite.NoError(err)
}

func (suite *HandlersSuite) TearDownTest() {
	suite.etcd.Close()
}

func (suite *HandlersSuite) TestLoadGrowingSegments() {
	ctx := context.Background()
	var err error
	// mock
	loadSegmetns := []int64{}
	var loadedInfos []*querypb.SegmentLoadInfo
	delegator := delegator.NewMockShardDelegator(suite.T())
	delegator.EXPECT().LoadGrowing(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) {
		loadedInfos = infos
		for _, info := range infos {
			loadSegmetns = append(loadSegmetns, info.SegmentID)
		}
	}).Return(nil)

	req := &querypb.WatchDmChannelsRequest{
		Infos: []*datapb.VchannelInfo{
			{
				CollectionID:        suite.collectionID,
				ChannelName:         suite.channel,
				UnflushedSegmentIds: []int64{suite.segmentID},
			},
		},
		SegmentInfos: make(map[int64]*datapb.SegmentInfo),
	}

	// unflushed segment not in segmentInfos, will skip
	err = loadGrowingSegments(ctx, delegator, req)
	suite.NoError(err)
	suite.Equal(0, len(loadSegmetns))

	// binlog was empty, will skip
	req.SegmentInfos[suite.segmentID] = &datapb.SegmentInfo{
		ID:           suite.segmentID,
		CollectionID: suite.collectionID,
		Binlogs:      make([]*datapb.FieldBinlog, 0),
	}
	err = loadGrowingSegments(ctx, delegator, req)
	suite.NoError(err)
	suite.Equal(0, len(loadSegmetns))

	// V3 storage: binlog is empty but ManifestPath is set, should load
	stats := &datapb.Statistics{
		InsertBinlogSize: 10,
		LoadResource: &datapb.LoadResourceStatistics{
			ColumnGroups: []*datapb.ColumnGroupStatistics{{GroupId: 0, FieldIds: []int64{100}, MemorySize: 10}},
		},
	}
	textStats := map[int64]*datapb.TextIndexStats{101: {MemorySize: 20}}
	jsonStats := map[int64]*datapb.JsonKeyStats{102: {}}
	req.SegmentInfos[suite.segmentID] = &datapb.SegmentInfo{
		ID:             suite.segmentID,
		CollectionID:   suite.collectionID,
		Binlogs:        make([]*datapb.FieldBinlog, 0),
		StorageVersion: storage.StorageV3,
		ManifestPath:   "files/binlogs/1/2/1000/manifest_0",
		Stats:          stats,
		TextStatsLogs:  textStats,
		JsonKeyStats:   jsonStats,
	}
	err = loadGrowingSegments(ctx, delegator, req)
	suite.NoError(err)
	suite.Equal(1, len(loadSegmetns))
	suite.Require().Len(loadedInfos, 1)
	suite.Same(stats, loadedInfos[0].GetStats())
	suite.Equal(textStats, loadedInfos[0].GetTextStatsLogs())
	suite.Equal(jsonStats, loadedInfos[0].GetJsonKeyStatsLogs())

	// normal load with binlogs
	loadSegmetns = loadSegmetns[:0]
	binlog := &datapb.FieldBinlog{}
	req.SegmentInfos[suite.segmentID].Binlogs = append(req.SegmentInfos[suite.segmentID].Binlogs, binlog)
	req.SegmentInfos[suite.segmentID].ManifestPath = ""
	err = loadGrowingSegments(ctx, delegator, req)
	suite.NoError(err)
	suite.Equal(1, len(loadSegmetns))
}

func TestHandlersSuite(t *testing.T) {
	suite.Run(t, new(HandlersSuite))
}

func TestQueryChannelReportsExecutedSnapshot(t *testing.T) {
	type collectionTarget struct{ segments.CollectionManager }
	type delegatorTarget struct{ delegator.ShardDelegator }
	paramtable.Init()
	for _, name := range []string{"rows", "empty", "query_error", "reduce_error", "channel_missing", "collection_missing", "remote_cost", "worker_cost"} {
		t.Run(name, func(t *testing.T) {
			manager := &collectionTarget{}
			sd := &delegatorTarget{}
			node := &QueryNode{manager: &segments.Manager{Collection: manager}, delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator]()}
			if name != "channel_missing" {
				node.delegators.Insert("ch0", sd)
			}
			key := paramtable.Get().QueryNodeCfg.EnableWorkerSQCostMetrics.Key
			paramtable.Get().Save(key, fmt.Sprint(name == "worker_cost"))
			t.Cleanup(func() { paramtable.Get().Reset(key) })
			ref := mockey.Mock((*collectionTarget).Ref).Return(name != "collection_missing").Build()
			defer ref.UnPatch()
			get := mockey.Mock((*collectionTarget).Get).Return(&segments.Collection{}).Build()
			defer get.UnPatch()
			unref := mockey.Mock((*collectionTarget).Unref).Return(true).Build()
			defer unref.UnPatch()
			query := mockey.Mock((*delegatorTarget).Query).To(func(_ *delegatorTarget, _ context.Context, req *querypb.QueryRequest) ([]*internalpb.RetrieveResults, error) {
				req.Req.MvccTimestamp = 80
				if name == "query_error" {
					return nil, merr.WrapErrServiceUnavailable("query failed")
				}
				if name == "empty" {
					return nil, nil
				}
				if name == "remote_cost" {
					return []*internalpb.RetrieveResults{{Base: &commonpb.MsgBase{SourceID: paramtable.GetNodeID() + 1}, CostAggregation: &internalpb.CostAggregation{TotalRelatedDataSize: 1}}}, nil
				}
				return []*internalpb.RetrieveResults{{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}}}, nil
			}).Build()
			defer query.UnPatch()
			reduce := mockey.Mock(segments.RunDelegatorQueryPipeline).To(func(_ context.Context, req *querypb.QueryRequest, _ *schemapb.CollectionSchema, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
				require.EqualValues(t, 80, req.Req.MvccTimestamp)
				if name == "reduce_error" {
					return nil, merr.WrapErrServiceUnavailable("reduce failed")
				}
				if len(results) == 0 {
					return &internalpb.RetrieveResults{Status: merr.Success()}, nil
				}
				return &internalpb.RetrieveResults{Status: merr.Success(), Ids: results[0].GetIds()}, nil
			}).Build()
			defer reduce.UnPatch()
			req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}, CollectionID: 1}, DmlChannels: []string{"ch0"}}
			result, err := node.queryChannel(context.Background(), req, "ch0")
			if name == "query_error" || name == "reduce_error" || name == "channel_missing" || name == "collection_missing" {
				require.Error(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 80, result.GetMvccTimestamp())
				if name == "empty" {
					require.Nil(t, result.GetIds())
				}
			}
		})
	}
}
