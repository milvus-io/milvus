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
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type QueryNodeSuite struct {
	suite.Suite
	// data
	address string

	// dependency
	params              *paramtable.ComponentParam
	node                *QueryNode
	etcd                *clientv3.Client
	chunkManagerFactory *storage.ChunkManagerFactory

	// mock
	factory *dependency.MockFactory
}

func (suite *QueryNodeSuite) SetupSuite() {
	suite.address = "test-address"
}

func (suite *QueryNodeSuite) SetupTest() {
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

func (suite *QueryNodeSuite) TearDownTest() {
	suite.etcd.Close()
}

func (suite *QueryNodeSuite) TestBasic() {
	// mock expect
	suite.factory.EXPECT().Init(mock.Anything).Return()
	suite.factory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(suite.chunkManagerFactory.NewPersistentStorageChunkManager(context.Background()))

	var err error
	suite.node.SetEtcdClient(suite.etcd)
	err = suite.node.Init()
	suite.NoError(err)

	// node should be unhealthy before node start
	suite.False(suite.node.lifetime.GetState() == commonpb.StateCode_Healthy)

	// start node
	err = suite.node.Start()
	suite.NoError(err)

	// node should be healthy after node start
	suite.True(suite.node.lifetime.GetState() == commonpb.StateCode_Healthy)

	// register node to etcd
	err = suite.node.Register()
	suite.NoError(err)

	// set and get address
	suite.node.SetAddress(suite.address)
	address := suite.node.GetAddress()
	suite.Equal(suite.address, address)

	// close node
	err = suite.node.Stop()
	suite.NoError(err)

	// node should be unhealthy after node stop
	suite.False(suite.node.lifetime.GetState() == commonpb.StateCode_Healthy)
}

func (suite *QueryNodeSuite) TestInit_RemoteChunkManagerFailed() {
	var err error
	suite.node.SetEtcdClient(suite.etcd)

	// init remote chunk manager failed
	suite.factory.EXPECT().Init(mock.Anything).Return()
	suite.factory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(nil, errors.New("mock error"))
	err = suite.node.Init()
	suite.Error(err)
}

func (suite *QueryNodeSuite) TestInit_VactorChunkManagerFailed() {
	var err error
	suite.node.SetEtcdClient(suite.etcd)

	// init vactor chunk manager failed
	suite.factory.EXPECT().Init(mock.Anything).Return()
	suite.factory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(nil, errors.New("mock error")).Once()
	err = suite.node.Init()
	suite.Error(err)
}

func (suite *QueryNodeSuite) TestInit_QueryHook() {
	// mock expect
	suite.factory.EXPECT().Init(mock.Anything).Return()
	suite.factory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(suite.chunkManagerFactory.NewPersistentStorageChunkManager(context.Background()))

	var err error
	suite.node.SetEtcdClient(suite.etcd)
	err = suite.node.Init()
	suite.NoError(err)

	mockHook := mock_optimizers.NewMockQueryHook(suite.T())
	suite.node.queryHook = mockHook
	suite.node.handleQueryHookEvent()

	configPath := "../../configs/milvus.yaml"
	origConfig, err := os.ReadFile(configPath)
	suite.Require().NoError(err)
	suite.T().Cleanup(func() {
		os.WriteFile(configPath, origConfig, 0o600) //nolint:gosec // configPath is a hardcoded test path
	})

	yamlWriter := newYamlConfigWriter(configPath)
	var x1, x2, x3 int32
	suite.Equal(atomic.LoadInt32(&x1), int32(0))
	suite.Equal(atomic.LoadInt32(&x2), int32(0))
	suite.Equal(atomic.LoadInt32(&x3), int32(0))

	mockHook.EXPECT().InitTuningConfig(mock.Anything).Run(func(params map[string]string) {
		atomic.StoreInt32(&x1, 6)
	}).Return(nil)

	// create tuning conf
	yamlWriter.set("autoIndex.params.tuning.1238", "xxxx")
	yamlWriter.writeConfig()
	suite.Eventually(func() bool {
		return atomic.LoadInt32(&x1) == int32(6)
	}, 20*time.Second, time.Second)

	mockHook.EXPECT().Init(mock.Anything).Run(func(params string) {
		atomic.StoreInt32(&x2, 5)
	}).Return(nil)
	yamlWriter.set("autoIndex.params.search", "aaaa")
	yamlWriter.writeConfig()
	suite.Eventually(func() bool {
		return atomic.LoadInt32(&x2) == int32(5)
	}, 20*time.Second, time.Second)
	yamlWriter.set("autoIndex.params.search", "")
	yamlWriter.writeConfig()

	atomic.StoreInt32(&x1, 0)
	suite.Equal(atomic.LoadInt32(&x1), int32(0))
	// update tuning conf
	yamlWriter.set("autoIndex.params.tuning.1238", "yyyy")
	yamlWriter.writeConfig()
	suite.Eventually(func() bool {
		return atomic.LoadInt32(&x1) == int32(6)
	}, 20*time.Second, time.Second)

	mockHook.EXPECT().DeleteTuningConfig(mock.Anything).Run(func(params string) {
		atomic.StoreInt32(&x3, 7)
	}).Return(nil)

	// delete tuning conf
	yamlWriter.set("autoIndex.params.tuning", "")
	yamlWriter.writeConfig()
	suite.Eventually(func() bool {
		return atomic.LoadInt32(&x3) == int32(7)
	}, 20*time.Second, time.Second)
}

func (suite *QueryNodeSuite) TestStop() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "2")

	suite.node.manager = segments.NewManager()

	schema := mock_segcore.GenTestCollectionSchema("test_stop", schemapb.DataType_Int64, true)
	collection, err := segments.NewCollection(1, schema, nil, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	suite.Require().NoError(err)
	segment, err := segments.NewSegment(
		context.Background(),
		collection,
		suite.node.manager.Segment,
		segments.SegmentTypeSealed,
		1,
		&querypb.SegmentLoadInfo{
			SegmentID:     100,
			PartitionID:   10,
			CollectionID:  1,
			Level:         datapb.SegmentLevel_Legacy,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", 1),
		},
	)
	suite.NoError(err)
	suite.node.manager.Segment.Put(context.Background(), segments.SegmentTypeSealed, segment)
	err = suite.node.Stop()
	suite.NoError(err)
	suite.True(suite.node.manager.Segment.Empty())
}

func (suite *QueryNodeSuite) TestHasOtherActiveQueryNode() {
	metaRoot := fmt.Sprintf("milvus-ut/test-has-other-active-qn/%d", time.Now().UnixNano())

	// Create and register the current node's session.
	sess := sessionutil.NewSessionWithEtcd(suite.node.ctx, metaRoot, suite.etcd)
	sess.Init(typeutil.QueryNodeRole, "addr1", false)
	sess.Register()
	defer sess.Stop()

	node := &QueryNode{
		ctx:      suite.node.ctx,
		session:  sess,
		serverID: sess.ServerID,
	}

	// No other node registered — should return false.
	suite.False(node.hasOtherActiveQueryNode())

	// Simulate a second active query node by writing session data directly to etcd.
	otherSessionKey := fmt.Sprintf("%s/session/%s-%d", metaRoot, typeutil.QueryNodeRole, 99999)
	activeSessionJSON := `{"ServerID":99999,"ServerName":"querynode","Address":"addr2","Stopping":false}`
	_, err := suite.etcd.Put(context.Background(), otherSessionKey, activeSessionJSON)
	suite.Require().NoError(err)
	defer suite.etcd.Delete(context.Background(), otherSessionKey)

	// Another active node exists — should return true.
	suite.True(node.hasOtherActiveQueryNode())

	// Update the other session to stopping state.
	stoppingSessionJSON := `{"ServerID":99999,"ServerName":"querynode","Address":"addr2","Stopping":true}`
	_, err = suite.etcd.Put(context.Background(), otherSessionKey, stoppingSessionJSON)
	suite.Require().NoError(err)

	// Only stopping node exists — should return false.
	suite.False(node.hasOtherActiveQueryNode())
}

func (suite *QueryNodeSuite) TestStopStandaloneMigrateTimeout() {
	// Set standalone role and a very short migrate timeout so test finishes quickly.
	paramtable.SetRole(typeutil.StandaloneRole)
	defer paramtable.SetRole("")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "10")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.StandaloneMigrateDataTimeout.Key, "1s")
	// Use a non-rocksmq WAL so the migrate data loop is entered.
	paramtable.Get().Save("mq.type", "pulsar")

	metaRoot := fmt.Sprintf("milvus-ut/test-standalone-migrate/%d", time.Now().UnixNano())

	// Set up a registered session on suite.node so GoingStop succeeds and the migrate loop runs.
	sess := sessionutil.NewSessionWithEtcd(suite.node.ctx, metaRoot, suite.etcd)
	sess.Init(typeutil.QueryNodeRole, "addr-standalone", false)
	sess.Register()
	suite.node.session = sess
	suite.node.serverID = sess.ServerID
	suite.node.manager = segments.NewManager()

	// Ensure segcore is initialized (needed by NewSegment).
	initcore.InitLocalChunkManager(suite.T().TempDir())
	err := initcore.InitMmapManager(paramtable.Get(), 1)
	suite.Require().NoError(err)

	// Put a sealed segment so the migrate data loop has something to wait for.
	schema := mock_segcore.GenTestCollectionSchema("test_standalone_stop", schemapb.DataType_Int64, true)
	collection, err := segments.NewCollection(1, schema, nil, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	suite.Require().NoError(err)
	segment, err := segments.NewSegment(
		context.Background(),
		collection,
		suite.node.manager.Segment,
		segments.SegmentTypeSealed,
		1,
		&querypb.SegmentLoadInfo{
			SegmentID:     200,
			PartitionID:   20,
			CollectionID:  1,
			Level:         datapb.SegmentLevel_Legacy,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", 1),
		},
	)
	suite.Require().NoError(err)
	suite.node.manager.Segment.Put(context.Background(), segments.SegmentTypeSealed, segment)

	// Stop should return within a few seconds (standalone migrate timeout = 1s),
	// NOT block for the full GracefulStopTimeout (10s).
	start := time.Now()
	err = suite.node.Stop()
	elapsed := time.Since(start)
	suite.NoError(err)
	suite.Less(elapsed, 5*time.Second, "standalone stop should exit early when no other active query node exists")
}

func TestResizeThreadPools(t *testing.T) {
	paramtable.Init()

	// not updated event should be no-op
	evt := &config.Event{HasUpdated: false}
	assert.NotPanics(t, func() { ResizeHighPriorityPool(evt) })
	assert.NotPanics(t, func() { ResizeMiddlePriorityPool(evt) })
	assert.NotPanics(t, func() { ResizeLowPriorityPool(evt) })
	assert.NotPanics(t, func() { ResizeAllPools(evt) })

	// updated event should resize without panic
	evt = &config.Event{HasUpdated: true}
	assert.NotPanics(t, func() { ResizeHighPriorityPool(evt) })
	assert.NotPanics(t, func() { ResizeMiddlePriorityPool(evt) })
	assert.NotPanics(t, func() { ResizeLowPriorityPool(evt) })
	assert.NotPanics(t, func() { ResizeAllPools(evt) })
}

func TestRegisterSegcoreConfigWatcher(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	node := &QueryNode{}

	assert.NotPanics(t, func() { node.RegisterSegcoreConfigWatcher() })

	// verify watchers are triggered by saving config values
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.HighPriorityThreadCoreCoefficient.Key, "10")
	})
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, "5")
	})
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.LowPriorityThreadCoreCoefficient.Key, "1")
	})
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.ThreadPoolMaxThreadsSize.Key, "32")
	})
}

func TestQueryNode(t *testing.T) {
	suite.Run(t, new(QueryNodeSuite))
}

// yamlConfigWriter is a minimal viper replacement for test config manipulation.
type yamlConfigWriter struct {
	path string
	data map[string]interface{}
}

func newYamlConfigWriter(path string) *yamlConfigWriter {
	w := &yamlConfigWriter{path: path}
	raw, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	w.data = make(map[string]interface{})
	if err := yaml.Unmarshal(raw, &w.data); err != nil {
		panic(err)
	}
	return w
}

func (w *yamlConfigWriter) set(key string, value interface{}) {
	parts := strings.Split(key, ".")
	m := w.data
	for _, p := range parts[:len(parts)-1] {
		child, ok := m[p]
		if !ok {
			child = make(map[string]interface{})
			m[p] = child
		}
		if cm, ok := child.(map[string]interface{}); ok {
			m = cm
		} else {
			nm := make(map[string]interface{})
			m[p] = nm
			m = nm
		}
	}
	m[parts[len(parts)-1]] = value
}

func (w *yamlConfigWriter) writeConfig() {
	out, err := yaml.Marshal(w.data)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(w.path, out, 0o600); err != nil {
		panic(err)
	}
}
