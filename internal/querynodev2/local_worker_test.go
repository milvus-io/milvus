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

	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type LocalWorkerTestSuite struct {
	suite.Suite
	params *paramtable.ComponentParam
	// data
	collectionID   int64
	collectionName string
	channel        string
	partitionIDs   []int64
	segmentIDs     []int64
	schema         *schemapb.CollectionSchema
	indexMeta      *segcorepb.CollectionIndexMeta

	// dependency
	node       *QueryNode
	worker     *LocalWorker
	etcdClient *clientv3.Client
	// context
	ctx    context.Context
	cancel context.CancelFunc
}

func (suite *LocalWorkerTestSuite) SetupSuite() {
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.channel = "test-channel"
	suite.partitionIDs = []int64{11, 22}
	suite.segmentIDs = []int64{0, 1}
}

func (suite *LocalWorkerTestSuite) BeforeTest(suiteName, testName string) {
	var err error
	// init param
	paramtable.Init()
	suite.params = paramtable.Get()
	// close GC at test to avoid data race
	suite.params.Save(suite.params.QueryNodeCfg.GCEnabled.Key, "false")

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	// init node
	factory := dependency.MockDefaultFactory(true, paramtable.Get())
	suite.node = NewQueryNode(suite.ctx, factory)
	//	init etcd
	suite.etcdClient, err = etcd.GetEtcdClient(
		suite.params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		suite.params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		suite.params.EtcdCfg.Endpoints.GetAsStrings(),
		suite.params.EtcdCfg.EtcdTLSCert.GetValue(),
		suite.params.EtcdCfg.EtcdTLSKey.GetValue(),
		suite.params.EtcdCfg.EtcdTLSCACert.GetValue(),
		suite.params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	suite.NoError(err)
	suite.node.SetEtcdClient(suite.etcdClient)
	err = suite.node.Init()
	suite.NoError(err)
	err = suite.node.Start()
	suite.NoError(err)

	suite.schema = segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	suite.indexMeta = segments.GenTestIndexMeta(suite.collectionID, suite.schema)
	collection := segments.NewCollection(suite.collectionID, suite.schema, suite.indexMeta, querypb.LoadType_LoadCollection)
	loadMata := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
	}
	suite.node.manager.Collection.PutOrRef(suite.collectionID, collection.Schema(), suite.indexMeta, loadMata)
	suite.worker = NewLocalWorker(suite.node)
}

func (suite *LocalWorkerTestSuite) AfterTest(suiteName, testName string) {
	suite.node.Stop()
	suite.etcdClient.Close()
	suite.cancel()
}

func (suite *LocalWorkerTestSuite) TestLoadSegment() {
	// load empty
	req := &querypb.LoadSegmentsRequest{
		CollectionID: suite.collectionID,
		Infos: lo.Map(suite.segmentIDs, func(segID int64, _ int) *querypb.SegmentLoadInfo {
			return &querypb.SegmentLoadInfo{
				CollectionID: suite.collectionID,
				PartitionID:  suite.partitionIDs[segID%2],
				SegmentID:    segID,
			}
		}),
	}
	err := suite.worker.LoadSegments(suite.ctx, req)
	suite.NoError(err)
}

func (suite *LocalWorkerTestSuite) TestReleaseSegment() {
	req := &querypb.ReleaseSegmentsRequest{
		CollectionID: suite.collectionID,
		SegmentIDs:   suite.segmentIDs,
	}
	err := suite.worker.ReleaseSegments(suite.ctx, req)
	suite.NoError(err)
}

func TestLocalWorker(t *testing.T) {
	suite.Run(t, new(LocalWorkerTestSuite))
}
