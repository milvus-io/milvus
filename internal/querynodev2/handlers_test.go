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
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	suite.chunkManagerFactory = storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test"))

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
	os.RemoveAll("/tmp/milvus-test")
}

func (suite *HandlersSuite) TestLoadGrowingSegments() {
	ctx := context.Background()
	var err error
	// mock
	loadSegmetns := []int64{}
	delegator := delegator.NewMockShardDelegator(suite.T())
	delegator.EXPECT().LoadGrowing(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) {
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

	// normal load
	binlog := &datapb.FieldBinlog{}
	req.SegmentInfos[suite.segmentID].Binlogs = append(req.SegmentInfos[suite.segmentID].Binlogs, binlog)
	err = loadGrowingSegments(ctx, delegator, req)
	suite.NoError(err)
	suite.Equal(1, len(loadSegmetns))
}

func TestHandlersSuite(t *testing.T) {
	suite.Run(t, new(HandlersSuite))
}
