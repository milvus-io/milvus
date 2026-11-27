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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	defer suite.etcd.Delete(context.Background(), metaRoot, clientv3.WithPrefix())

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
	hasOther, err := node.hasOtherActiveQueryNode()
	suite.NoError(err)
	suite.False(hasOther)

	// Another active node exists — should return true.
	suite.putQueryNodeSession(metaRoot, 99999, false, nil)
	hasOther, err = node.hasOtherActiveQueryNode()
	suite.NoError(err)
	suite.True(hasOther)

	// Only stopping node exists — should return false.
	suite.putQueryNodeSession(metaRoot, 99999, true, nil)
	hasOther, err = node.hasOtherActiveQueryNode()
	suite.NoError(err)
	suite.False(hasOther)

	// An active query node embedded in a streaming node means the cluster is still serving.
	suite.putQueryNodeSession(metaRoot, 99998, false, map[string]string{sessionutil.LabelStreamingNodeEmbeddedQueryNode: "1"})
	hasOther, err = node.hasOtherActiveQueryNode()
	suite.NoError(err)
	suite.True(hasOther)

	// Context cancellation is an unknown answer, not "no active node".
	canceledCtx, cancel := context.WithCancel(node.ctx)
	cancel()
	node.ctx = canceledCtx
	hasOther, err = node.hasOtherActiveQueryNode()
	suite.Error(err)
	suite.False(hasOther)

	// A lookup error is returned at once; the migrate loop retries on its next tick.
	node.ctx = context.Background()
	attempts := 0
	patch := mockey.Mock((*sessionutil.Session).GetSessions).To(func(_ *sessionutil.Session, _ context.Context, _ string) (map[string]*sessionutil.Session, int64, error) {
		attempts++
		return nil, 0, errors.New("transient session lookup failure")
	}).Build()
	defer patch.UnPatch()

	hasOther, err = node.hasOtherActiveQueryNode()
	suite.Error(err)
	suite.False(hasOther)
	suite.Equal(1, attempts)
}

// putQueryNodeSession writes a query node session directly to etcd.
func (suite *QueryNodeSuite) putQueryNodeSession(metaRoot string, serverID int64, stopping bool, labels map[string]string) {
	value, err := json.Marshal(&sessionutil.SessionRaw{
		ServerID:     serverID,
		ServerName:   typeutil.QueryNodeRole,
		Address:      fmt.Sprintf("addr-%d", serverID),
		Stopping:     stopping,
		ServerLabels: labels,
	})
	suite.Require().NoError(err)
	key := fmt.Sprintf("%s/session/%s-%d", metaRoot, typeutil.QueryNodeRole, serverID)
	_, err = suite.etcd.Put(context.Background(), key, string(value))
	suite.Require().NoError(err)
}

// prepareMigratingNode registers suite.node under metaRoot and loads a sealed
// segment, so Stop enters the migrate data loop and checks for other active
// query nodes right away. The caller deletes metaRoot from etcd.
func (suite *QueryNodeSuite) prepareMigratingNode(metaRoot string, gracefulStopTimeout string) {
	params := paramtable.Get()
	params.Save(params.QueryNodeCfg.GracefulStopTimeout.Key, gracefulStopTimeout)
	params.Save(params.QueryNodeCfg.MigrateDataTimeout.Key, "0s")
	// Use a non-rocksmq WAL so the migrate data loop is entered.
	params.Save("mq.type", "pulsar")
	suite.T().Cleanup(func() {
		params.Reset(params.QueryNodeCfg.GracefulStopTimeout.Key)
		params.Reset(params.QueryNodeCfg.MigrateDataTimeout.Key)
		params.Reset("mq.type")
	})

	// Set up a registered session on suite.node so GoingStop succeeds and the migrate loop runs.
	sess := sessionutil.NewSessionWithEtcd(suite.node.ctx, metaRoot, suite.etcd)
	sess.Init(typeutil.QueryNodeRole, "addr-stopping", false)
	sess.Register()
	suite.node.session = sess
	suite.node.serverID = sess.ServerID
	suite.node.manager = segments.NewManager()

	// Ensure segcore is initialized (needed by NewSegment).
	initcore.InitLocalChunkManager(suite.T().TempDir())
	err := initcore.InitMmapManager(paramtable.Get(), 1)
	suite.Require().NoError(err)

	// Put a sealed segment so the migrate data loop has something to wait for.
	schema := mock_segcore.GenTestCollectionSchema("test_migrate_stop", schemapb.DataType_Int64, true)
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
}

func (suite *QueryNodeSuite) TestStopWithoutOtherActiveQueryNode() {
	for _, role := range []string{typeutil.StandaloneRole, typeutil.QueryNodeRole} {
		suite.Run(role, func() {
			paramtable.SetRole(role)
			defer paramtable.SetRole("")
			metaRoot := fmt.Sprintf("milvus-ut/test-stop-no-other-qn/%s/%d", role, time.Now().UnixNano())
			defer suite.etcd.Delete(context.Background(), metaRoot, clientv3.WithPrefix())
			suite.node = NewQueryNode(context.Background(), suite.factory)
			suite.prepareMigratingNode(metaRoot, "10")
			suite.putQueryNodeSession(metaRoot, 99999, true, nil)

			// Stop should exit as soon as the first successful session lookup confirms
			// there is no other active query node.
			start := time.Now()
			err := suite.node.Stop()
			elapsed := time.Since(start)
			suite.NoError(err)
			suite.Less(elapsed, time.Second, "stop should exit after the first successful session lookup without other active query node")
		})
	}
}

func (suite *QueryNodeSuite) TestStopWaitsUntilOtherQueryNodeStops() {
	metaRoot := fmt.Sprintf("milvus-ut/test-stop-other-qn-stops/%d", time.Now().UnixNano())
	defer suite.etcd.Delete(context.Background(), metaRoot, clientv3.WithPrefix())
	suite.prepareMigratingNode(metaRoot, "10")
	embedded := map[string]string{sessionutil.LabelStreamingNodeEmbeddedQueryNode: "1"}
	suite.putQueryNodeSession(metaRoot, 99999, false, embedded)

	// The streaming node starts stopping later, as when a whole cluster is stopped.
	go func() {
		time.Sleep(time.Second)
		suite.putQueryNodeSession(metaRoot, 99999, true, embedded)
	}()

	start := time.Now()
	err := suite.node.Stop()
	elapsed := time.Since(start)
	suite.NoError(err)
	suite.GreaterOrEqual(elapsed, time.Second, "stop should keep migrating while another active query node exists")
	suite.Less(elapsed, 5*time.Second, "stop should exit once the other query node is stopping")
}

func (suite *QueryNodeSuite) TestStopKeepsMigratingOnLookupFailure() {
	for _, role := range []string{typeutil.StandaloneRole, typeutil.QueryNodeRole} {
		suite.Run(role, func() {
			paramtable.SetRole(role)
			defer paramtable.SetRole("")
			metaRoot := fmt.Sprintf("milvus-ut/test-stop-lookup-failure/%s/%d", role, time.Now().UnixNano())
			defer suite.etcd.Delete(context.Background(), metaRoot, clientv3.WithPrefix())
			suite.node = NewQueryNode(context.Background(), suite.factory)
			suite.prepareMigratingNode(metaRoot, "2")
			attempts := atomic.Int32{}
			patch := mockey.Mock((*sessionutil.Session).GetSessions).To(func(_ *sessionutil.Session, _ context.Context, _ string) (map[string]*sessionutil.Session, int64, error) {
				attempts.Add(1)
				return nil, 0, errors.New("etcd unavailable")
			}).Build()
			defer patch.UnPatch()

			// A failed lookup must not skip migration, and must not block the loop,
			// so the graceful stop timeout ends it.
			start := time.Now()
			err := suite.node.Stop()
			elapsed := time.Since(start)
			suite.NoError(err)
			suite.GreaterOrEqual(elapsed, 2*time.Second)
			suite.Less(elapsed, 5*time.Second)
			suite.Greater(attempts.Load(), int32(0))
		})
	}
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
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, "32768")
	})
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, "1048576")
	})
	assert.NotPanics(t, func() {
		pt.Save(pt.CommonCfg.LoadTransientBudgetBytes.Key, "67108864")
	})
	pt.Reset(pt.CommonCfg.LoadTransientBudgetBytes.Key)
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
