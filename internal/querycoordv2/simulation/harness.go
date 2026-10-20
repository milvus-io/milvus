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

//go:build test

// Package simulation runs the real QueryCoord placement machinery (checkers,
// task scheduler/executor, distribution pulls, target and collection
// observers, balancers) against in-memory QueryNodes and an in-memory
// DataCoord. Time is real but scaled: every QueryCoord interval is shortened
// and simulated loads take milliseconds, so scenarios that span minutes of
// production behavior finish in seconds.
package simulation

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	snbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

// ScaledParams shortens QueryCoord intervals roughly tenfold. Scenarios may
// override any key through Options.Params.
func ScaledParams() map[string]string {
	p := &paramtable.Get().QueryCoordCfg
	return map[string]string{
		p.SegmentCheckInterval.Key:       "300",
		p.ChannelCheckInterval.Key:       "300",
		p.BalanceCheckInterval.Key:       "100",
		p.IndexCheckInterval.Key:         "1000",
		p.LeaderViewUpdateInterval.Key:   "1",
		p.DistPullInterval.Key:           "50",
		p.DispatchInterval.Key:           "50",
		p.AutoBalanceInterval.Key:        "300",
		p.UpdateNextTargetInterval.Key:   "1",
		p.NextTargetSurviveTime.Key:      "30",
		p.CollectionObserverInterval.Key: "50",
		p.CheckNodeInReplicaInterval.Key: "1",
		p.SegmentTaskTimeout.Key:         "6000",
		p.SegmentTaskRetryTimeout.Key:    "30000",
		p.LoadBandwidthPerCoreMBps.Key:   "320", // 32 MiB/s per core at 10x time
		p.ChannelTaskTimeout.Key:         "6000",
		p.ChannelTaskRetryTimeout.Key:    "30000",
		p.HeartbeatAvailableInterval.Key: "2000",
		// Simulated time runs about 10x fast, but segment Version (the load
		// time the cooldown reads) is wall clock: scale the 10 min cooldown.
		"queryCoord.twoLevel.cooldownSeconds": "60",
	}
}

type Options struct {
	// Params overrides paramtable keys after ScaledParams is applied.
	Params map[string]string
	// NodeDownDetectDelay models session expiry: how long QueryCoord keeps
	// believing a crashed node is alive.
	NodeDownDetectDelay time.Duration
	// Streaming enables streaming mode (StreamingNodes host WALs and
	// delegators). Nil runs the classic QueryNode-only topology.
	Streaming *StreamingOptions
}

// queryCoord is one incarnation of QueryCoord. A restart builds a new one
// over the same KV, broker and fleet.
type queryCoord struct {
	ctx    context.Context
	cancel context.CancelFunc

	nodeMgr   *session.NodeManager
	store     querycoord.Catalog
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	sched     task.Scheduler
	checkers  *checkers.CheckerController
	targetObs *observers.TargetObserver
	collObs   *observers.CollectionObserver
	replObs   *observers.ReplicaObserver
	resObs    *observers.ResourceObserver
	leaderObs *observers.LeaderCacheObserver
	distCtl   *dist.ControllerImpl
}

// Sim is a simulated cluster: one QueryCoord, a fleet of QueryNodes and a
// DataCoord.
type Sim struct {
	t      testing.TB
	opts   Options
	ctx    context.Context
	cancel context.CancelFunc

	Broker *Broker
	Fleet  *Fleet
	wal    *WALBalancer
	kv     kv.MetaKv
	nextID atomic.Int64

	mu       sync.Mutex
	qc       *queryCoord
	restarts int
	tr       *Trace // on with MILVUS_SIM_TRACE
}

type noopProxyManager struct {
	proxyutil.ProxyClientManagerInterface
}

func (noopProxyManager) InvalidateShardLeaderCache(context.Context, *proxypb.InvalidateShardLeaderCacheRequest) error {
	return nil
}

func (noopProxyManager) InvalidateCollectionMetaCache(context.Context, *proxypb.InvalidateCollMetaCacheRequest, ...proxyutil.ExpireCacheOpt) error {
	return nil
}

func New(t *testing.T, opts Options) *Sim {
	paramtable.Init()
	for key, value := range ScaledParams() {
		paramtable.Get().Save(key, value)
	}
	for key, value := range opts.Params {
		paramtable.Get().Save(key, value)
	}
	// MILVUS_SIM_PARAMS ("key=value,key=value") overrides every scenario, so
	// the same suite can run against another balancer or tuning.
	for _, kv := range strings.Split(os.Getenv("MILVUS_SIM_PARAMS"), ",") {
		if key, value, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
			paramtable.Get().Save(key, value)
		}
	}
	broker := NewBroker()
	var wal *WALBalancer
	if opts.Streaming != nil {
		if opts.Streaming.PChannels > 0 {
			broker.PChannels = opts.Streaming.PChannels
		}
		streamingutil.SetStreamingServiceEnabled()
		t.Cleanup(streamingutil.UnsetStreamingServiceEnabled)
		snmanager.ResetStreamingNodeManager()
		wal = newWALBalancer(broker.PChannels)
		snbalance.Register(wal)
	} else {
		streamingutil.UnsetStreamingServiceEnabled()
		snmanager.ResetDoNothingStreamingNodeManager(t)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Sim{
		t:      t,
		opts:   opts,
		ctx:    ctx,
		cancel: cancel,
		Broker: broker,
		Fleet:  NewFleet(broker),
		wal:    wal,
		kv:     tikvkv.NewTiKV(tikv.SetupLocalTxn(), fmt.Sprintf("sim/%s/%d", t.Name(), time.Now().UnixNano())),
	}
	s.nextID.Store(time.Now().UnixNano() / 1000)
	s.tr = startTrace(s)
	s.qc = s.boot()
	t.Cleanup(s.Close)
	return s
}

func (s *Sim) allocID() (int64, error) {
	return s.nextID.Inc(), nil
}

func (s *Sim) boot() *queryCoord {
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.ResetTwoLevelStateForTest()
	balance.ResetGlobalBalancerFactoryForTest()

	ctx, cancel := context.WithCancel(s.ctx)
	qc := &queryCoord{ctx: ctx, cancel: cancel}
	qc.nodeMgr = session.NewNodeManager()
	qc.nodeMgr.Start(ctx)
	qc.store = querycoord.NewCatalog(s.kv)
	qc.meta = meta.NewMeta(s.allocID, qc.store, qc.nodeMgr, s.Broker.Store)
	s.must(qc.meta.CollectionManager.Recover(ctx, s.Broker))
	s.must(qc.meta.ReplicaManager.Recover(ctx, qc.meta.GetAll(ctx)))
	s.must(qc.meta.ResourceManager.Recover(ctx))
	qc.dist = meta.NewDistributionManager(qc.nodeMgr)
	qc.targetMgr = meta.NewTargetManager(s.Broker, qc.meta, s.Broker.Store)
	_ = qc.targetMgr.Recover(ctx, qc.store)

	qc.sched = task.NewScheduler(ctx, qc.meta, qc.dist, qc.targetMgr, s.Broker, s.Fleet, qc.nodeMgr)
	if s.tr != nil {
		qc.sched = tracedScheduler{Scheduler: qc.sched, tr: s.tr}
	}
	assign.InitGlobalAssignPolicyFactory(qc.sched, qc.nodeMgr, qc.dist, qc.meta, qc.targetMgr)
	balance.InitGlobalBalancerFactory(qc.sched, qc.nodeMgr, qc.dist, qc.targetMgr)
	qc.checkers = checkers.NewCheckerController(qc.meta, qc.dist, qc.targetMgr, qc.nodeMgr, qc.sched, s.Broker)

	proxies := noopProxyManager{}
	qc.targetObs = observers.NewTargetObserver(qc.meta, qc.targetMgr, qc.dist, s.Broker, s.Fleet, qc.nodeMgr)
	qc.collObs = observers.NewCollectionObserver(qc.dist, qc.meta, qc.targetMgr, qc.targetObs, qc.checkers, proxies)
	qc.replObs = observers.NewReplicaObserver(qc.meta, qc.dist, qc.targetMgr)
	qc.resObs = observers.NewResourceObserver(qc.meta)
	qc.leaderObs = observers.NewLeaderCacheObserver(proxies)
	qc.distCtl = dist.NewDistController(s.Fleet, qc.nodeMgr, qc.dist, qc.targetMgr, qc.sched, qc.leaderObs.RegisterEvent)
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()

	// Rewatch: every node whose process is up registers again.
	for id, view := range s.Fleet.Snapshot() {
		if view.Up {
			s.registerNode(qc, id)
		}
	}
	qc.meta.CheckNodesInResourceGroup(ctx)

	qc.leaderObs.Start(ctx)
	qc.distCtl.SyncAll(ctx)
	qc.collObs.Start()
	qc.targetObs.Start()
	qc.replObs.Start()
	qc.resObs.Start()
	qc.sched.Start()
	qc.checkers.Start()
	return qc
}

func (s *Sim) shutdown(qc *queryCoord) {
	qc.checkers.Stop()
	qc.collObs.Stop()
	qc.targetObs.Stop()
	qc.replObs.Stop()
	qc.resObs.Stop()
	qc.leaderObs.Stop()
	qc.targetMgr.SaveCurrentTarget(qc.ctx, qc.store)
	qc.distCtl.Stop()
	qc.sched.Stop()
	qc.cancel()
}

func (s *Sim) Close() {
	s.tr.finish()
	s.mu.Lock()
	qc := s.qc
	s.qc = nil
	s.mu.Unlock()
	if qc != nil {
		s.shutdown(qc)
	}
	s.cancel()
}

// RestartQueryCoord stops QueryCoord and builds a new incarnation from the
// persisted metadata. QueryNodes keep their state and in-flight work.
func (s *Sim) RestartQueryCoord() {
	// The mark takes a keyframe, which reads the current QueryCoord: it
	// must not run under the lock that guards it.
	s.tr.mark("restart_qc")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdown(s.qc)
	s.restarts++
	s.qc = s.boot()
}

func (s *Sim) current() *queryCoord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.qc
}

func (s *Sim) must(err error) {
	if err != nil {
		s.t.Fatalf("simulation setup failed: %v", err)
	}
}

func (s *Sim) registerNode(qc *queryCoord, id int64) {
	labels := map[string]string{}
	if n := s.Fleet.node(id); n != nil && n.streaming {
		labels[sessionutil.LabelStreamingNodeEmbeddedQueryNode] = "1"
	}
	qc.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   id,
		Address:  fmt.Sprintf("sim-qn-%d", id),
		Hostname: fmt.Sprintf("sim-qn-%d", id),
		Version:  semver.MustParse("3.0.0"),
		Labels:   labels,
	}))
	qc.sched.AddExecutor(id)
	qc.distCtl.StartDistInstance(qc.ctx, id)
	qc.meta.HandleNodeUp(qc.ctx, id)
	qc.checkers.Check()
}

func (s *Sim) unregisterNode(qc *queryCoord, id int64) {
	qc.nodeMgr.Remove(id)
	qc.sched.RemoveExecutor(id)
	qc.distCtl.Remove(id)
	qc.dist.ChannelDistManager.Update(id)
	qc.dist.SegmentDistManager.Update(id)
	qc.sched.RemoveByNode(id)
	qc.meta.HandleNodeDown(context.Background(), id)
}

// AddNodes starts count QueryNodes with ids from firstID and registers them.
func (s *Sim) AddNodes(firstID int64, count int, cfg NodeConfig) []int64 {
	ids := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		id := firstID + int64(i)
		s.Fleet.AddNode(id, cfg)
		s.tr.addNode(id, cfg)
		s.registerNode(s.current(), id)
		ids = append(ids, id)
	}
	s.tr.mark(fmt.Sprintf("add_qn %v", ids))
	return ids
}

// KillNode crashes a QueryNode; QueryCoord learns of it after
// NodeDownDetectDelay.
func (s *Sim) KillNode(id int64) {
	s.tr.mark(fmt.Sprintf("kill_qn %d", id))
	s.Fleet.Kill(id)
	notify := func() { s.unregisterNode(s.current(), id) }
	if s.opts.NodeDownDetectDelay > 0 {
		time.AfterFunc(s.opts.NodeDownDetectDelay, notify)
	} else {
		notify()
	}
}

// RestartNode brings a killed QueryNode back empty and registers it.
func (s *Sim) RestartNode(id int64) {
	s.tr.mark(fmt.Sprintf("restart_qn %d", id))
	s.Fleet.Restart(id)
	s.registerNode(s.current(), id)
}

// StopNode gracefully stops a QueryNode: it is marked stopping, keeps serving
// until QueryCoord drains it (or timeout), then exits. It returns when the
// node has exited and reports whether the drain completed in time.
func (s *Sim) StopNode(id int64, timeout time.Duration) bool {
	s.tr.mark(fmt.Sprintf("stop_qn %d", id))
	s.Fleet.MarkStopping(id)
	qc := s.current()
	qc.nodeMgr.Stopping(id)
	qc.meta.HandleNodeStopping(context.Background(), id)
	qc.checkers.Check()
	start := time.Now()
	drained := s.WaitUntil(timeout, func() bool {
		v := s.Fleet.Snapshot()[id]
		return len(v.Segments) == 0 && len(v.Delegators) == 0
	})
	v := s.Fleet.Snapshot()[id]
	mlog.Info(context.Background(), "sim drain finished", mlog.Int64("node", id), mlog.Bool("drained", drained),
		mlog.Duration("elapsed", time.Since(start)), mlog.Int("segmentsLeft", len(v.Segments)),
		mlog.Int("delegatorsLeft", len(v.Delegators)))
	s.KillNode(id)
	return drained
}

// LoadCollection creates a collection in DataCoord and loads it with the
// given replica number, following the steps of LoadCollectionJob.
func (s *Sim) LoadCollection(id int64, replicas int32, numChannels, segmentsPerChannel int, rowsPerSegment int64) {
	s.LoadCollectionInRGs(id, []string{meta.DefaultResourceGroupName}, replicas, numChannels, segmentsPerChannel, rowsPerSegment)
}

// LoadCollectionInRGs is LoadCollection with explicit resource groups.
func (s *Sim) LoadCollectionInRGs(id int64, rgs []string, replicas int32, numChannels, segmentsPerChannel int, rowsPerSegment int64) {
	s.Broker.CreateCollection(id, numChannels, segmentsPerChannel, rowsPerSegment)
	s.loadInRGs(id, rgs, replicas)
}

// loadExisting loads a collection already created in the broker.
func (s *Sim) loadExisting(id int64, replicas int32) {
	s.loadInRGs(id, []string{meta.DefaultResourceGroupName}, replicas)
}

func (s *Sim) loadInRGs(id int64, rgs []string, replicas int32) {
	qc := s.current()
	ctx := qc.ctx
	_, err := utils.SpawnReplicasWithRG(ctx, qc.meta, id, rgs, replicas,
		s.Broker.Channels(id), commonpb.LoadPriority_HIGH)
	s.must(err)
	partition := s.Broker.Partition(id)
	s.must(qc.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  id,
			ReplicaNumber: replicas,
			Status:        querypb.LoadStatus_Loading,
			LoadType:      querypb.LoadType_LoadCollection,
		},
		CreatedAt: time.Now(),
	}, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID:  id,
			PartitionID:   partition,
			ReplicaNumber: replicas,
			Status:        querypb.LoadStatus_Loading,
		},
		CreatedAt: time.Now(),
	}))
	if _, err := qc.targetObs.UpdateNextTarget(id); err != nil {
		s.must(merr.Wrap(err, "update next target"))
	}
	qc.collObs.LoadPartitions(ctx, id, []int64{partition})
}

// ReleaseCollection releases a loaded collection, following
// ReleaseCollectionJob; the checkers then release its segments and channels.
func (s *Sim) ReleaseCollection(id int64) {
	qc := s.current()
	ctx := qc.ctx
	if qc.meta.Exist(ctx, id) {
		s.must(qc.meta.CollectionManager.RemoveCollection(ctx, id))
		qc.targetObs.ReleaseCollection(id)
	}
	s.must(qc.meta.ReplicaManager.RemoveCollection(ctx, id))
	qc.checkers.Check()
}

// liveNodeIDs lists the QueryNodes that are up and not stopping, oldest first.
func (s *Sim) liveNodeIDs() []int64 {
	var out []int64
	for id, v := range s.Fleet.Snapshot() {
		if v.Up && !s.Fleet.IsStopping(id) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// CreateResourceGroup adds a resource group that requests nodes from the
// default group, and waits until the resource observer has moved them.
func (s *Sim) CreateResourceGroup(name string, nodes int32) {
	qc := s.current()
	_, err := qc.meta.ResourceManager.AddResourceGroup(qc.ctx, name, meta.NewResourceGroupConfig(nodes, nodes))
	s.must(err)
	if !s.WaitUntil(10*time.Second, func() bool {
		got, err := s.current().meta.ResourceManager.GetNodes(s.Ctx(), name)
		return err == nil && len(got) == int(nodes)
	}) {
		s.t.Fatalf("resource group %s did not receive %d nodes", name, nodes)
	}
}

// ResizeResourceGroup changes how many nodes a resource group requests (and
// may hold) by delta, and waits for the resource observer to move nodes.
func (s *Sim) ResizeResourceGroup(name string, delta int) {
	qc := s.current()
	rg := qc.meta.ResourceManager.GetResourceGroup(qc.ctx, name)
	if rg == nil {
		s.t.Fatalf("resource group %s not found", name)
	}
	want := int(rg.GetConfig().GetRequests().GetNodeNum()) + delta
	s.must(qc.meta.ResourceManager.AlterResourceGroups(qc.ctx, map[string]*rgpb.ResourceGroupConfig{name: meta.NewResourceGroupConfig(int32(want), int32(want))}))
	s.WaitUntil(10*time.Second, func() bool {
		got, err := s.current().meta.ResourceManager.GetNodes(s.Ctx(), name)
		return err == nil && len(got) == want
	})
}

// NodeResourceGroup is the resource group a node belongs to.
func (s *Sim) NodeResourceGroup(id int64) string {
	qc := s.current()
	if qc == nil {
		return ""
	}
	return qc.meta.ResourceManager.GetResourceGroupByNodeID(id)
}

// RGNodes returns the nodes of a resource group.
func (s *Sim) RGNodes(name string) []int64 {
	nodes, _ := s.current().meta.ResourceManager.GetNodes(s.Ctx(), name)
	return nodes
}

// Loaded reports whether every given collection is Loaded.
func (s *Sim) Loaded(ids ...int64) bool {
	qc := s.current()
	for _, id := range ids {
		c := qc.meta.GetCollection(qc.ctx, id)
		if c == nil || c.GetStatus() != querypb.LoadStatus_Loaded {
			return false
		}
	}
	return true
}

// PendingTasks returns the scheduler's segment and channel task counts.
func (s *Sim) PendingTasks() (int, int) {
	qc := s.current()
	return qc.sched.GetSegmentTaskNum(), qc.sched.GetChannelTaskNum()
}

// WaitUntil polls cond every 20ms until it holds or timeout elapses.
func (s *Sim) WaitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Meta exposes QueryCoord metadata for scenario assertions.
func (s *Sim) Meta() *meta.Meta { return s.current().meta }

// Targets exposes the target manager for scenario assertions.
func (s *Sim) Targets() *meta.TargetManager { return s.current().targetMgr }

// Ctx returns the context of the current QueryCoord incarnation.
func (s *Sim) Ctx() context.Context { return s.current().ctx }
