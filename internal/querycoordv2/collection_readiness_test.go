// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type readinessCatalog struct {
	views []*viewpb.QueryViewOfShard
}

func (c *readinessCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return c.views, nil
}

func (c *readinessCatalog) SaveQueryViews(context.Context, []*viewpb.QueryViewOfShard) error {
	return nil
}

type readinessSyncer struct{ sent chan syncer.SyncView }

func (s *readinessSyncer) SyncViews(_ context.Context, group syncer.SyncGroup) error {
	for _, views := range group.ViewsByNode {
		for _, view := range views {
			s.sent <- view
		}
	}
	return nil
}
func (*readinessSyncer) Close() error { return nil }

func newReadinessTestServer(t *testing.T, views ...*viewpb.QueryViewOfShard) (*Server, *metastoremocks.QueryCoordCatalog, *readinessSyncer) {
	t.Helper()
	return newReadinessTestServerWithConfig(t, &querypb.CollectionLoadInfo{CollectionID: 100}, views...)
}

func newReadinessTestServerWithConfig(t *testing.T, info *querypb.CollectionLoadInfo, views ...*viewpb.QueryViewOfShard) (*Server, *metastoremocks.QueryCoordCatalog, *readinessSyncer) {
	t.Helper()
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{info}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{{ID: 1000, CollectionID: 100}}, nil).Once()
	store, err := loadmgr.RecoverLoadConfigStore(ctx, catalog)
	require.NoError(t, err)
	syncer := &readinessSyncer{sent: make(chan syncer.SyncView, 64)}
	registry, err := coordview.RecoverShardViewRegistry(ctx, &readinessCatalog{views: views}, syncer)
	require.NoError(t, err)
	runtime := &qviewsRuntime{loadConfigStore: store, shardViewRegistry: registry, readyChanges: newCollectionReadiness(store, registry)}
	t.Cleanup(func() { runtime.readyChanges.Close(); registry.Close() })
	s := &Server{ctx: ctx, qviewsRuntime: runtime}
	s.collectionUsage = newCollectionUsageManager(store, nil)
	t.Cleanup(s.collectionUsage.close)
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	return s, catalog, syncer
}

func readinessRequest(channels ...string) *querypb.EnsureCollectionReadyRequest {
	return &querypb.EnsureCollectionReadyRequest{CollectionID: 100, ExpectedVchannels: channels}
}

func receiveReadinessSync(t *testing.T, s *readinessSyncer, state qviews.QueryViewState) syncer.SyncView {
	t.Helper()
	select {
	case view := <-s.sent:
		require.Equal(t, state, view.View.State())
		return view
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for node sync")
		return syncer.SyncView{}
	}
}

func TestEnsureCollectionReadyWakesFromStreamingNodeUp(t *testing.T) {
	first, second := "by-dev-rootcoord-dml_100v0", "by-dev-rootcoord-dml_100v1"
	s, _, syncer := newReadinessTestServer(t, testPersistedQueryView(100, qviews.ShardID{ReplicaID: 1000, VChannel: first}))
	req := readinessRequest(first, second)
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { status, err := s.EnsureCollectionReady(ctx, req); result <- merr.CheckRPCCall(status, err) }()
	assertReadinessBlocked(t, result)
	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: second}
	builder := qviews.NewQueryViewAtCoordBuilder(1000, &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: second}},
	}, second).SetLoadInfoVersion(s.qviewsRuntime.loadConfigStore.GetConfigVersion(100))
	require.NoError(t, s.qviewsRuntime.shardViewRegistry.Ensure(shardID).AddPreparing(ctx, builder))
	preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
	ready := preparing.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
	assertReadinessBlocked(t, result)
	// Drive the real Coord state machine, registry observer and RPC waiter from
	// the SN's Up acknowledgement; do not directly notify the test waiter.
	up.OnSyncResponse(up.View)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("SN Up did not wake the readiness RPC")
	}
	status, err := s.EnsureCollectionReady(ctx, req)
	require.NoError(t, merr.CheckRPCCall(status, err), "an already-ready collection needs no new notification")
}

func TestEnsureCollectionReadyWaitsForLoadConfigVersion(t *testing.T) {
	for _, path := range []string{"ensure", "wait"} {
		t.Run(path, func(t *testing.T) {
			const channel = "by-dev-rootcoord-dml_100v0"
			shardID := qviews.ShardID{ReplicaID: 1000, VChannel: channel}
			s, catalog, syncer := newReadinessTestServer(t, testPersistedQueryView(100, shardID))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			config := s.qviewsRuntime.loadConfigStore.Get(100).Config.Clone()
			config.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 102}}
			catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
			catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
			require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(ctx, config))
			entry := s.qviewsRuntime.loadConfigStore.Get(100)
			require.NotEqual(t, uint64(1), entry.ConfigVersion)

			client := newEnsureRPCClient(t, s)
			result := make(chan error, 1)
			go func() {
				if path == "wait" {
					result <- s.waitCollectionReady(ctx, readinessRequest(channel))
					return
				}
				status, err := client.EnsureCollectionReady(ctx, readinessRequest(channel))
				result <- merr.CheckRPCCall(status, err)
			}()
			assertReadinessBlocked(t, result)
			builder := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, &viewpb.DataViewOfCollection{
				CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				Shards: []*viewpb.DataViewOfShard{{Vchannel: channel}},
			}, channel).SetLoadInfoVersion(entry.ConfigVersion)
			require.NoError(t, s.qviewsRuntime.shardViewRegistry.Ensure(shardID).AddPreparing(ctx, builder))
			preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
			ready := preparing.View.IntoProto()
			ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
			preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
			up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
			assertReadinessBlocked(t, result)
			up.OnSyncResponse(up.View)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("matching load config version did not release the waiter")
			}
		})
	}
}

// Done is evaluated by waitCollectionReady after checking the current config.
// This gives the test a barrier before changing the config of a pending wait.
type readinessWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *readinessWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestWaitCollectionReadyTracksLoadConfigChangesWhileWaiting(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_100v0"
	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: channel}
	s, catalog, syncer := newReadinessTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initial := s.qviewsRuntime.loadConfigStore.Get(100)
	waitCtx := &readinessWaitContext{Context: ctx, waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		result <- s.waitCollectionReady(waitCtx, readinessRequest(channel))
	}()
	select {
	case <-waitCtx.waiting:
	case <-ctx.Done():
		t.Fatal("readiness did not reach its first wait")
	}
	config := initial.Config.Clone()
	config.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 102}}
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(ctx, config))
	current := s.qviewsRuntime.loadConfigStore.Get(100)
	require.NotEqual(t, initial.ConfigVersion, current.ConfigVersion)
	for _, loadInfoVersion := range []uint64{initial.ConfigVersion, current.ConfigVersion} {
		builder := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, &viewpb.DataViewOfCollection{
			CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1},
			Shards: []*viewpb.DataViewOfShard{{Vchannel: channel}},
		}, channel).SetLoadInfoVersion(loadInfoVersion)
		require.NoError(t, s.qviewsRuntime.shardViewRegistry.Ensure(shardID).AddPreparing(ctx, builder))
		preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
		ready := preparing.View.IntoProto()
		ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
		preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
		up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
		assertReadinessBlocked(t, result)
		up.OnSyncResponse(up.View)
		if loadInfoVersion == initial.ConfigVersion {
			assertReadinessBlocked(t, result)
		}
	}
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("latest load config version did not release the waiter")
	}
}

func assertReadinessBlocked(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("readiness returned before the collection was ready: %v", err)
	case <-time.After(35 * time.Millisecond):
	}
}

func TestEnsureCollectionReadyEndsOnReleaseCancelAndShutdown(t *testing.T) {
	for _, scenario := range []string{"release", "cancel", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			s, catalog, _ := newReadinessTestServer(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.ensureCollectionReady(ctx, readinessRequest("by-dev-rootcoord-dml_100v0")) }()
			assertReadinessBlocked(t, result)
			var want error = merr.ErrCollectionNotLoaded
			switch scenario {
			case "release":
				catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
				catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
				require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(ctx, 100))
			case "cancel":
				cancel()
				want = context.Canceled
			case "shutdown":
				s.qviewsRuntime.readyChanges.Close()
				want = merr.ErrServiceUnavailable
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, want)
			case <-time.After(time.Second):
				t.Fatal("waiter was not released")
			}
		})
	}
}

func TestEnsureCollectionReadyGRPCContract(t *testing.T) {
	s, catalog, _ := newReadinessTestServer(t)
	broker := meta.NewMockBroker(t)
	s.broker = broker
	broker.EXPECT().DescribeCollection(mock.Anything, int64(200)).Return(nil, merr.WrapErrCollectionNotFound(200)).Once()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	querypb.RegisterQueryCoordServer(grpcServer, s)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///readiness", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := querypb.NewQueryCoordClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = client.EnsureCollectionReady(ctx, readinessRequest("by-dev-rootcoord-dml_100v0"))
	require.Error(t, err, "a real RPC must honor its deadline")
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(context.Background(), 100))
	status, err := client.EnsureCollectionReady(context.Background(), &querypb.EnsureCollectionReadyRequest{
		CollectionID: 200, ExpectedVchannels: []string{"by-dev-rootcoord-dml_200v0"},
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrCollectionNotFound, "automatic load must preserve collection lookup errors over RPC")
}

func TestEnsureCollectionReadyCancellationDoesNotWaitForCatalog(t *testing.T) {
	s, catalog, _ := newReadinessTestServer(t)
	writeStarted, finishWrite := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	finish := func() { finishOnce.Do(func() { close(finishWrite) }) }
	defer finish()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Run(func(context.Context, *querypb.CollectionLoadInfo, ...*querypb.PartitionLoadInfo) {
		close(writeStarted)
		<-finishWrite
	}).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- s.qviewsRuntime.loadConfigStore.Put(context.Background(), s.qviewsRuntime.loadConfigStore.Get(100).Config)
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("catalog write did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.ensureCollectionReady(ctx, readinessRequest("by-dev-rootcoord-dml_100v0")) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("readiness cancellation waited on a blocked catalog write")
	}
	finish()
	require.NoError(t, <-writeResult)
}

func TestEnsureCollectionReadySharesLoadAcrossRPCs(t *testing.T) {
	total := testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel))
	success := testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel))
	fail := testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel))
	s, catalog, broker := newLoadConfigQViewsServer(t)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	s.ctx = ctx
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	s.qviewsRuntime.shardViewRegistry.Close()
	_ = s.qviewsRuntime.syncer.Close()
	syncer := &readinessSyncer{sent: make(chan syncer.SyncView, 64)}
	registry, err := coordview.RecoverShardViewRegistry(ctx, &readinessCatalog{}, syncer)
	require.NoError(t, err)
	s.qviewsRuntime.shardViewRegistry = registry
	s.qviewsRuntime.readyChanges.Close()
	s.qviewsRuntime.readyChanges = newCollectionReadiness(s.qviewsRuntime.loadConfigStore, registry)
	t.Cleanup(func() { s.qviewsRuntime.readyChanges.Close(); registry.Close() })

	const channel = "by-dev-rootcoord-dml_100v0"
	coll := testDescribeCollection(100, []string{channel})
	coll.Schema.Fields = []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
	}
	broker.EXPECT().DescribeCollection(mock.Anything, int64(100)).Return(coll, nil).Times(3)
	broker.EXPECT().GetPartitions(mock.Anything, int64(100)).Return([]int64{10}, nil).Once()
	broker.EXPECT().GetCollectionLoadInfo(mock.Anything, int64(100)).Return([]string{meta.DefaultResourceGroupName}, int64(1), nil).Once()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()

	mix := mocks.NewMixCoord(t)
	s.mixCoord = mix
	loading := make(chan context.Context, 1)
	proceed := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(proceed) }) }
	t.Cleanup(release)
	mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
			loading <- ctx
			select {
			case <-proceed:
				return &indexpb.DescribeIndexResponse{Status: merr.Success(), IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 1000}}}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Once()
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) {
		result := message.BroadcastResultAlterLoadConfigMessageV2{Message: message.MustAsBroadcastAlterLoadConfigMessageV2(msg)}
		require.NoError(t, s.qviewsRuntime.loadManager.UpdateLoadConfig(ctx, result))
	})

	// Independent gRPC connections model callers from different Proxy processes.
	first := newEnsureRPCClient(t, s)
	second := newEnsureRPCClient(t, s)
	request := func() *querypb.EnsureCollectionReadyRequest {
		return &querypb.EnsureCollectionReadyRequest{CollectionID: 100, ExpectedVchannels: []string{channel}}
	}
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		status, err := first.EnsureCollectionReady(firstCtx, request())
		firstDone <- merr.CheckRPCCall(status, err)
	}()
	var sharedCtx context.Context
	select {
	case sharedCtx = <-loading:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic load did not start")
	}
	const callers = 12
	done := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			status, err := second.EnsureCollectionReady(ctx, request())
			done <- merr.CheckRPCCall(status, err)
		}()
	}
	cancelFirst()
	select {
	case err := <-firstDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("canceled caller did not return")
	}
	require.NoError(t, sharedCtx.Err(), "the first caller must not own the shared load")
	release()
	require.Eventually(t, func() bool { return s.qviewsRuntime.loadConfigStore.Contains(100) }, 3*time.Second, time.Millisecond)
	assertReadinessBlocked(t, done)
	config := s.qviewsRuntime.loadConfigStore.GetConfig(100)
	require.Len(t, config.LoadFields, 2)
	require.EqualValues(t, 1000, config.LoadFields[1].GetIndexId())
	shardID := qviews.ShardID{ReplicaID: config.Replicas[0].ReplicaID, VChannel: channel}
	builder := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: channel}},
	}, channel).SetLoadInfoVersion(s.qviewsRuntime.loadConfigStore.GetConfigVersion(100))
	require.NoError(t, registry.Ensure(shardID).AddPreparing(ctx, builder))
	preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
	ready := preparing.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
	assertReadinessBlocked(t, done)
	up.OnSyncResponse(up.View)
	for i := 0; i < callers; i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("ready collection did not release all callers")
		}
	}
	// An already-ready collection submits no additional load.
	status, err := second.EnsureCollectionReady(ctx, request())
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Equal(t, total+1, testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel)))
	require.Equal(t, success+1, testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel)))
	require.Equal(t, fail, testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel)))
}

func newEnsureRPCClient(t *testing.T, s *Server) querypb.QueryCoordClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	querypb.RegisterQueryCoordServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///ensure-ready", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return querypb.NewQueryCoordClient(conn)
}

func TestAutoLoadAppliesDefaultsAfterConcurrentLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, catalog, broker := newLoadConfigQViewsServer(t)
	s.ctx = context.Background()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	t.Cleanup(s.qviewsRuntime.stop)
	const channel = "by-dev-rootcoord-dml_100v0"
	coll := testDescribeCollection(100, []string{channel})
	coll.Schema.Fields = []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
		{FieldID: 102, Name: "scalar", DataType: schemapb.DataType_Int64},
	}
	broker.EXPECT().DescribeCollection(mock.Anything, int64(100)).Return(coll, nil).Times(3)
	broker.EXPECT().GetPartitions(mock.Anything, int64(100)).Return([]int64{10}, nil).Once()
	broker.EXPECT().GetCollectionLoadInfo(mock.Anything, int64(100)).Return([]string{meta.DefaultResourceGroupName}, int64(1), nil).Once()
	mix := mocks.NewMixCoord(t)
	s.mixCoord = mix
	mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
		&indexpb.DescribeIndexResponse{Status: merr.Success(), IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 1000}}}, nil).Once()
	config := &loadmgr.LoadConfig{
		DbID: 1, CollectionID: 100, PartitionIDs: []int64{10}, UserSpecifiedReplicaMode: true,
		LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 1000}},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 1000, ResourceGroup: meta.DefaultResourceGroupName, Priority: commonpb.LoadPriority_HIGH},
			{ReplicaID: 1001, ResourceGroup: meta.DefaultResourceGroupName, Priority: commonpb.LoadPriority_HIGH},
			{ReplicaID: 1002, ResourceGroup: meta.DefaultResourceGroupName, Priority: commonpb.LoadPriority_HIGH},
		},
	}
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().ReleaseReplica(mock.Anything, int64(100), int64(1001), int64(1002)).Return(nil).Once()

	// The retained replica already has an Up view; the extra concurrent replicas do not.
	s.qviewsRuntime.shardViewRegistry.Close()
	_ = s.qviewsRuntime.syncer.Close()
	syncer := &readinessSyncer{sent: make(chan syncer.SyncView, 64)}
	registry, err := coordview.RecoverShardViewRegistry(ctx, &readinessCatalog{
		views: []*viewpb.QueryViewOfShard{testPersistedQueryView(100, qviews.ShardID{ReplicaID: 1000, VChannel: channel})},
	}, syncer)
	require.NoError(t, err)
	s.qviewsRuntime.shardViewRegistry = registry
	s.qviewsRuntime.readyChanges.Close()
	s.qviewsRuntime.readyChanges = newCollectionReadiness(s.qviewsRuntime.loadConfigStore, registry)

	configCommitted := make(chan struct{})
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			result := message.BroadcastResultAlterLoadConfigMessageV2{Message: message.MustAsBroadcastAlterLoadConfigMessageV2(msg)}
			if err := s.qviewsRuntime.loadManager.UpdateLoadConfig(ctx, result); err != nil {
				return nil, err
			}
			close(configCommitted)
			return &types.BroadcastAppendResult{}, nil
		}).Once()
	bapi.EXPECT().Close().Return().Once()
	bc := mock_broadcaster.NewMockBroadcaster(t)
	bc.EXPECT().Close().Return().Maybe()
	bc.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			// Simulate a manual load committing while automatic load waits for the DDL lock.
			require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(ctx, config))
			return bapi, nil
		}).Once()
	broadcast.ResetBroadcaster()
	broadcast.Register(bc)
	t.Cleanup(broadcast.ResetBroadcaster)
	client := newEnsureRPCClient(t, s)
	result := make(chan error, 1)
	go func() {
		status, err := client.EnsureCollectionReady(ctx, readinessRequest(channel))
		result <- merr.CheckRPCCall(status, err)
	}()
	select {
	case <-configCommitted:
	case <-ctx.Done():
		t.Fatal("automatic load did not commit its config")
	}
	assertReadinessBlocked(t, result)
	entry := s.qviewsRuntime.loadConfigStore.Get(100)
	loaded := entry.Config
	require.Equal(t, []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 1000}, {FieldId: 102}}, loaded.LoadFields)
	require.Len(t, loaded.Replicas, 1)
	require.EqualValues(t, 1000, loaded.Replicas[0].ReplicaID)
	require.Equal(t, meta.DefaultResourceGroupName, loaded.Replicas[0].ResourceGroup)
	require.False(t, loaded.UserSpecifiedReplicaMode)
	shardID := qviews.ShardID{ReplicaID: loaded.Replicas[0].ReplicaID, VChannel: channel}
	builder := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
		Shards: []*viewpb.DataViewOfShard{{Vchannel: channel}},
	}, channel).SetLoadInfoVersion(entry.ConfigVersion)
	require.NoError(t, registry.Ensure(shardID).AddPreparing(ctx, builder))
	preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
	ready := preparing.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
	assertReadinessBlocked(t, result)
	up.OnSyncResponse(up.View)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("automatic load did not wait for the view with its default fields")
	}
}

func TestPrepareAutoLoadRequestFieldsAndIndexErrors(t *testing.T) {
	for _, scenario := range []string{"fields", "missing index", "rpc error"} {
		t.Run(scenario, func(t *testing.T) {
			mix := mocks.NewMixCoord(t)
			s := &Server{mixCoord: mix}
			coll := testDescribeCollection(100, nil)
			coll.Schema.Fields = []*schemapb.FieldSchema{
				{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
				{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
				{FieldID: 102, Name: "skipped", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}},
			}
			resp := &indexpb.DescribeIndexResponse{Status: merr.Success()}
			if scenario == "fields" {
				resp.IndexInfos = []*indexpb.IndexInfo{{FieldID: 101, IndexID: 1000}}
			}
			if scenario == "rpc error" {
				resp.Status = merr.Status(merr.ErrServiceUnavailable)
			}
			mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(resp, nil).Once()
			req, err := s.prepareAutoLoadRequest(context.Background(), coll)
			switch scenario {
			case "fields":
				require.NoError(t, err)
				require.Equal(t, []int64{100, 101}, req.GetLoadFields())
				require.Equal(t, map[int64]int64{101: 1000}, req.GetFieldIndexID())
				require.Equal(t, commonpb.LoadPriority_HIGH, req.GetPriority())
			case "missing index":
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			case "rpc error":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
		})
	}
}

func TestEnsureCollectionReadyLoadFailureCanBeRetried(t *testing.T) {
	total := testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel))
	fail := testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel))
	s, _, broker := newLoadConfigQViewsServer(t)
	s.ctx = context.Background()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	t.Cleanup(s.qviewsRuntime.stop)
	const channel = "by-dev-rootcoord-dml_100v0"
	coll := testDescribeCollection(100, []string{channel})
	broker.EXPECT().DescribeCollection(mock.Anything, int64(100)).Return(coll, nil).Times(5)
	broker.EXPECT().GetPartitions(mock.Anything, int64(100)).Return([]int64{10}, nil).Once()
	broker.EXPECT().GetCollectionLoadInfo(mock.Anything, int64(100)).Return([]string{meta.DefaultResourceGroupName}, int64(1), nil).Once()
	mix := mocks.NewMixCoord(t)
	s.mixCoord = mix
	mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
		&indexpb.DescribeIndexResponse{Status: merr.Status(merr.ErrServiceUnavailable)}, nil).Once()
	mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
		&indexpb.DescribeIndexResponse{Status: merr.Status(merr.ErrIndexNotFound)}, nil).Once()
	mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(
		&indexpb.DescribeIndexResponse{Status: merr.Success()}, nil).Once()
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnavailable).Once()
	bapi.EXPECT().Close().Return().Once()
	bc := mock_broadcaster.NewMockBroadcaster(t)
	bc.EXPECT().Close().Return().Maybe()
	bc.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).Return(bapi, nil).Once()
	broadcast.ResetBroadcaster()
	broadcast.Register(bc)
	t.Cleanup(broadcast.ResetBroadcaster)
	client := newEnsureRPCClient(t, s)
	// The final attempt reaches ordinary LoadCollection and exercises its Status round trip.
	for _, want := range []error{merr.ErrServiceUnavailable, merr.ErrIndexNotFound, merr.ErrServiceUnavailable} {
		resp, err := client.EnsureCollectionReady(context.Background(), &querypb.EnsureCollectionReadyRequest{
			CollectionID: 100, ExpectedVchannels: []string{channel},
		})
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp), want)
		require.Equal(t, merr.Code(want), resp.GetCode())
		require.Equal(t, merr.IsRetryableErr(want), resp.GetRetriable())
		require.False(t, s.qviewsRuntime.loadConfigStore.Contains(100))
	}
	require.Equal(t, total+1, testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel)))
	require.Equal(t, fail+1, testutil.ToFloat64(metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel)))
}
