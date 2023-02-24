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

package querynode

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/cockroachdb/errors"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type shardClusterState int32

const (
	available   shardClusterState = 1
	unavailable shardClusterState = 2
)

type nodeEventType int32

const (
	nodeAdd nodeEventType = 1
	nodeDel nodeEventType = 2
)

type segmentEventType int32

const (
	segmentAdd segmentEventType = 1
	segmentDel segmentEventType = 2
)

type segmentState int32

const (
	segmentStateNone    segmentState = 0
	segmentStateOffline segmentState = 1
	segmentStateLoading segmentState = 2
	segmentStateLoaded  segmentState = 3
)

type nodeEvent struct {
	eventType nodeEventType
	nodeID    int64
	nodeAddr  string
	isLeader  bool
}

type segmentEvent struct {
	eventType   segmentEventType
	segmentID   int64
	partitionID int64
	nodeIDs     []int64 // nodes from events
	state       segmentState
}

type shardQueryNode interface {
	GetStatistics(context.Context, *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error)
	Search(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error)
	Query(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	Stop() error
}

type shardNode struct {
	nodeID   int64
	nodeAddr string
	client   shardQueryNode
}

type shardSegmentInfo struct {
	segmentID   int64
	partitionID int64
	nodeID      int64
	state       segmentState
	version     int64
	inUse       int32
}

// Closable interface for close.
type Closable interface {
	Close()
}

// ShardNodeDetector provides method to detect node events
type ShardNodeDetector interface {
	Closable
	watchNodes(collectionID int64, replicaID int64, vchannelName string) ([]nodeEvent, <-chan nodeEvent)
}

// ShardSegmentDetector provides method to detect segment events
type ShardSegmentDetector interface {
	Closable
	watchSegments(collectionID int64, replicaID int64, vchannelName string) ([]segmentEvent, <-chan segmentEvent)
}

// ShardNodeBuilder function type to build types.QueryNode from addr and id
type ShardNodeBuilder func(nodeID int64, addr string) shardQueryNode

// withStreaming function type to let search detects corresponding search streaming is done.
type searchWithStreaming func(ctx context.Context) (error, *internalpb.SearchResults)
type queryWithStreaming func(ctx context.Context) (error, *internalpb.RetrieveResults)
type getStatisticsWithStreaming func(ctx context.Context) (error, *internalpb.GetStatisticsResponse)

// ShardCluster maintains the ShardCluster information and perform shard level operations
type ShardCluster struct {
	state *atomic.Int32

	collectionID int64
	replicaID    int64
	vchannelName string
	version      int64

	nodeDetector    ShardNodeDetector
	segmentDetector ShardSegmentDetector
	nodeBuilder     ShardNodeBuilder

	mut      sync.RWMutex
	leader   *shardNode           // shard leader node instance
	nodes    map[int64]*shardNode // online nodes
	segments SegmentsStatus       // shard segments

	mutVersion     sync.RWMutex
	versions       sync.Map             // version id to version
	currentVersion *ShardClusterVersion // current serving segment state version
	nextVersionID  *atomic.Int64
	segmentCond    *sync.Cond // segment state change condition

	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewShardCluster create a ShardCluster with provided information.
func NewShardCluster(collectionID int64, replicaID int64, vchannelName string, version int64,
	nodeDetector ShardNodeDetector, segmentDetector ShardSegmentDetector, nodeBuilder ShardNodeBuilder) *ShardCluster {
	sc := &ShardCluster{
		state: atomic.NewInt32(int32(unavailable)),

		collectionID: collectionID,
		replicaID:    replicaID,
		vchannelName: vchannelName,
		version:      version,

		segmentDetector: segmentDetector,
		nodeDetector:    nodeDetector,
		nodeBuilder:     nodeBuilder,

		nodes:         make(map[int64]*shardNode),
		segments:      make(map[int64]shardSegmentInfo),
		nextVersionID: atomic.NewInt64(0),

		closeCh: make(chan struct{}),
	}

	m := sync.Mutex{}
	sc.segmentCond = sync.NewCond(&m)

	sc.init()

	return sc
}

func (sc *ShardCluster) Close() {
	log := sc.getLogger()
	log.Info("Close shard cluster")
	sc.closeOnce.Do(func() {
		sc.updateShardClusterState(unavailable)
		if sc.nodeDetector != nil {
			sc.nodeDetector.Close()
		}
		if sc.segmentDetector != nil {
			sc.segmentDetector.Close()
		}

		close(sc.closeCh)
	})
}

func (sc *ShardCluster) getLogger() *log.MLogger {
	return log.With(
		zap.Int64("collectionID", sc.collectionID),
		zap.String("channel", sc.vchannelName),
		zap.Int64("replicaID", sc.replicaID),
	)
}

// serviceable returns whether shard cluster could provide query service.
func (sc *ShardCluster) serviceable() bool {
	// all segment in loaded state
	if sc.state.Load() != int32(available) {
		return false
	}

	sc.mutVersion.RLock()
	defer sc.mutVersion.RUnlock()
	// check there is a working version(SyncSegments called)
	return sc.currentVersion != nil
}

// addNode add a node into cluster
func (sc *ShardCluster) addNode(evt nodeEvent) {
	log := sc.getLogger()
	log.Info("ShardCluster add node", zap.Int64("nodeID", evt.nodeID))
	sc.mut.Lock()
	defer sc.mut.Unlock()

	oldNode, ok := sc.nodes[evt.nodeID]
	if ok {
		if oldNode.nodeAddr == evt.nodeAddr {
			log.Warn("ShardCluster add same node, skip", zap.Int64("nodeID", evt.nodeID), zap.String("addr", evt.nodeAddr))
			return
		}
		defer oldNode.client.Stop()
	}

	node := &shardNode{
		nodeID:   evt.nodeID,
		nodeAddr: evt.nodeAddr,
		client:   sc.nodeBuilder(evt.nodeID, evt.nodeAddr),
	}
	sc.nodes[evt.nodeID] = node
	if evt.isLeader {
		sc.leader = node
	}
}

// removeNode handles node offline and setup related segments
func (sc *ShardCluster) removeNode(evt nodeEvent) {
	log := sc.getLogger()
	log.Info("ShardCluster remove node", zap.Int64("nodeID", evt.nodeID))
	sc.mut.Lock()
	defer sc.mut.Unlock()

	old, ok := sc.nodes[evt.nodeID]
	if !ok {
		log.Warn("ShardCluster removeNode does not belong to it", zap.Int64("nodeID", evt.nodeID), zap.String("addr", evt.nodeAddr))
		return
	}

	defer old.client.Stop()
	delete(sc.nodes, evt.nodeID)

	for id, segment := range sc.segments {
		if segment.nodeID == evt.nodeID {
			segment.state = segmentStateOffline
			segment.version = -1
			sc.segments[id] = segment
			sc.updateShardClusterState(unavailable)
		}
	}
	// ignore leader process here
}

// updateSegment apply segment change to shard cluster
func (sc *ShardCluster) updateSegment(evt shardSegmentInfo) {
	log := sc.getLogger()
	log.Info("ShardCluster update segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))
	// notify handoff wait online if any
	defer func() {
		sc.segmentCond.Broadcast()
	}()

	sc.mut.Lock()
	defer sc.mut.Unlock()

	old, ok := sc.segments[evt.segmentID]
	if !ok { // newly add
		sc.segments[evt.segmentID] = evt
		return
	}

	sc.transferSegment(old, evt)
}

// SetupFirstVersion initialized first version for shard cluster.
func (sc *ShardCluster) SetupFirstVersion() {
	sc.mutVersion.Lock()
	defer sc.mutVersion.Unlock()
	version := NewShardClusterVersion(sc.nextVersionID.Inc(), make(SegmentsStatus), nil)
	sc.versions.Store(version.versionID, version)
	sc.currentVersion = version
}

// SyncSegments synchronize segment distribution in batch
func (sc *ShardCluster) SyncSegments(distribution []*querypb.ReplicaSegmentsInfo, state segmentState) {
	log := sc.getLogger()
	log.Info("ShardCluster sync segments", zap.Any("replica segments", distribution), zap.Int32("state", int32(state)))

	var currentVersion *ShardClusterVersion
	sc.mutVersion.RLock()
	currentVersion = sc.currentVersion
	sc.mutVersion.RUnlock()
	if currentVersion == nil {
		log.Warn("received SyncSegments call before version setup")
		return
	}

	sc.mut.Lock()
	for _, line := range distribution {
		for i, segmentID := range line.GetSegmentIds() {
			nodeID := line.GetNodeId()
			version := line.GetVersions()[i]
			// if node id not in replica node list, this line shall be placeholder for segment offline
			_, ok := sc.nodes[nodeID]
			if !ok {
				log.Warn("Sync segment with invalid nodeID", zap.Int64("segmentID", segmentID), zap.Int64("nodeID", line.NodeId))
				nodeID = common.InvalidNodeID
			}

			old, ok := sc.segments[segmentID]
			if !ok { // newly add
				sc.segments[segmentID] = shardSegmentInfo{
					nodeID:      nodeID,
					partitionID: line.GetPartitionId(),
					segmentID:   segmentID,
					state:       state,
					version:     version,
				}
				continue
			}

			sc.transferSegment(old, shardSegmentInfo{
				nodeID:      nodeID,
				partitionID: line.GetPartitionId(),
				segmentID:   segmentID,
				state:       state,
				version:     version,
			})
		}
	}

	//	allocations := sc.segments.Clone(filterNothing)
	sc.mut.Unlock()

	// notify handoff wait online if any
	sc.segmentCond.Broadcast()

	sc.mutVersion.Lock()
	defer sc.mutVersion.Unlock()

	// update shardleader allocation view
	allocations := sc.currentVersion.segments.Clone(filterNothing)
	for _, line := range distribution {
		for _, segmentID := range line.GetSegmentIds() {
			allocations[segmentID] = shardSegmentInfo{nodeID: line.GetNodeId(), segmentID: segmentID, partitionID: line.GetPartitionId(), state: state}
		}
	}

	version := NewShardClusterVersion(sc.nextVersionID.Inc(), allocations, sc.currentVersion)
	sc.versions.Store(version.versionID, version)
	sc.currentVersion = version
}

// transferSegment apply segment state transition.
// old\new | Offline | Loading | Loaded
// Offline | OK		 | OK	   | OK
// Loading | OK		 | OK	   | NodeID check
// Loaded  | OK      | OK	   | legacy pending
func (sc *ShardCluster) transferSegment(old shardSegmentInfo, evt shardSegmentInfo) {
	log := sc.getLogger()
	switch old.state {
	case segmentStateOffline: // safe to update nodeID and state
		old.nodeID = evt.nodeID
		old.state = evt.state
		old.version = evt.version
		sc.segments[old.segmentID] = old
		if evt.state == segmentStateLoaded {
			sc.healthCheck()
		}
	case segmentStateLoading: // to Loaded only when nodeID equal
		if evt.state == segmentStateLoaded && evt.nodeID != old.nodeID {
			log.Warn("transferSegment to loaded failed, nodeID not match", zap.Int64("segmentID", evt.segmentID), zap.Int64("nodeID", old.nodeID), zap.Int64("evtNodeID", evt.nodeID))
			return
		}
		old.nodeID = evt.nodeID
		old.state = evt.state
		old.version = evt.version
		sc.segments[old.segmentID] = old
		if evt.state == segmentStateLoaded {
			sc.healthCheck()
		}
	case segmentStateLoaded:
		// load balance
		old.nodeID = evt.nodeID
		old.state = evt.state
		old.version = evt.version
		sc.segments[old.segmentID] = old
		if evt.state != segmentStateLoaded {
			sc.healthCheck()
		}
	}
}

// removeSegment removes segment from cluster
// should only applied in hand-off or load balance procedure
func (sc *ShardCluster) removeSegment(evt shardSegmentInfo) {
	log := sc.getLogger()
	log.Info("ShardCluster remove segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))

	sc.mut.Lock()
	defer sc.mut.Unlock()

	old, ok := sc.segments[evt.segmentID]
	if !ok {
		log.Warn("ShardCluster removeSegment does not belong to it", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID))
		return
	}

	if old.nodeID != evt.nodeID {
		log.Warn("ShardCluster removeSegment found node not match", zap.Int64("segmentID", evt.segmentID), zap.Int64("nodeID", old.nodeID), zap.Int64("evtNodeID", evt.nodeID))
		return
	}

	delete(sc.segments, evt.segmentID)
	sc.healthCheck()
}

// init list all nodes and semgent states ant start watching
func (sc *ShardCluster) init() {
	// list nodes
	nodes, nodeEvtCh := sc.nodeDetector.watchNodes(sc.collectionID, sc.replicaID, sc.vchannelName)
	for _, node := range nodes {
		sc.addNode(node)
	}
	go sc.watchNodes(nodeEvtCh)

	// list segments
	segments, segmentEvtCh := sc.segmentDetector.watchSegments(sc.collectionID, sc.replicaID, sc.vchannelName)
	for _, segment := range segments {
		info, ok := sc.pickNode(segment)
		if ok {
			sc.updateSegment(info)
		}
	}
	go sc.watchSegments(segmentEvtCh)

	sc.healthCheck()
}

// pickNode selects node in the cluster
func (sc *ShardCluster) pickNode(evt segmentEvent) (shardSegmentInfo, bool) {
	nodeID, has := sc.selectNodeInReplica(evt.nodeIDs)
	if has { // assume one segment shall exist once in one replica
		return shardSegmentInfo{
			segmentID:   evt.segmentID,
			partitionID: evt.partitionID,
			nodeID:      nodeID,
			state:       evt.state,
		}, true
	}

	return shardSegmentInfo{}, false
}

// selectNodeInReplica returns first node id inside the shard cluster replica.
// if there is no nodeID found, returns 0.
func (sc *ShardCluster) selectNodeInReplica(nodeIDs []int64) (int64, bool) {
	for _, nodeID := range nodeIDs {
		_, has := sc.getNode(nodeID)
		if has {
			return nodeID, true
		}
	}
	return 0, false
}

func (sc *ShardCluster) updateShardClusterState(state shardClusterState) {
	log := sc.getLogger()
	old := sc.state.Load()
	sc.state.Store(int32(state))

	pc, _, _, _ := runtime.Caller(1)
	callerName := runtime.FuncForPC(pc).Name()

	log.Info("Shard Cluster update state",
		zap.Int32("old state", old), zap.Int32("new state", int32(state)),
		zap.String("caller", callerName))
}

// healthCheck iterate all segments to to check cluster could provide service.
func (sc *ShardCluster) healthCheck() {
	for _, segment := range sc.segments {
		if segment.state != segmentStateLoaded ||
			segment.nodeID == common.InvalidNodeID { // segment in offline nodes
			sc.updateShardClusterState(unavailable)
			return
		}
	}
	sc.updateShardClusterState(available)
}

// watchNodes handles node events.
func (sc *ShardCluster) watchNodes(evtCh <-chan nodeEvent) {
	log := sc.getLogger()
	for {
		select {
		case evt, ok := <-evtCh:
			if !ok {
				log.Warn("ShardCluster node channel closed")
				return
			}
			switch evt.eventType {
			case nodeAdd:
				sc.addNode(evt)
			case nodeDel:
				sc.removeNode(evt)
			}
		case <-sc.closeCh:
			log.Info("ShardCluster watchNode quit")
			return
		}
	}
}

// watchSegments handles segment events.
func (sc *ShardCluster) watchSegments(evtCh <-chan segmentEvent) {
	log := sc.getLogger()
	for {
		select {
		case evt, ok := <-evtCh:
			if !ok {
				log.Warn("ShardCluster segment channel closed")
				return
			}
			info, ok := sc.pickNode(evt)
			if !ok {
				log.Info("No node of event is in cluster, skip to process it",
					zap.Int64s("nodes", evt.nodeIDs))
				continue
			}
			switch evt.eventType {
			case segmentAdd:
				sc.updateSegment(info)
			case segmentDel:
				sc.removeSegment(info)
			}
		case <-sc.closeCh:
			log.Info("ShardCluster watchSegments quit")
			return
		}
	}
}

// getNode returns shallow copy of shardNode
func (sc *ShardCluster) getNode(nodeID int64) (*shardNode, bool) {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	node, ok := sc.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return &shardNode{
		nodeID:   node.nodeID,
		nodeAddr: node.nodeAddr,
		client:   node.client, // shallow copy
	}, true
}

// getSegment returns copy of shardSegmentInfo
func (sc *ShardCluster) getSegment(segmentID int64) (shardSegmentInfo, bool) {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	segment, ok := sc.segments[segmentID]
	return segment, ok
}

// segmentAllocations returns node to segments mappings.
// calling this function also increases the reference count of related segments.
func (sc *ShardCluster) segmentAllocations(partitionIDs []int64) (map[int64][]int64, int64) {
	// check cluster serviceable
	if !sc.serviceable() {
		log.Warn("request segment allocations when cluster is not serviceable", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID), zap.String("vchannelName", sc.vchannelName))
		return map[int64][]int64{}, 0
	}
	sc.mutVersion.RLock()
	defer sc.mutVersion.RUnlock()
	// return allocation from current version and version id
	return sc.currentVersion.GetAllocation(partitionIDs), sc.currentVersion.versionID
}

// finishUsage decreases the inUse count of provided segments
func (sc *ShardCluster) finishUsage(versionID int64) {
	v, ok := sc.versions.Load(versionID)
	if !ok {
		return
	}

	version := v.(*ShardClusterVersion)
	version.FinishUsage()

	// cleanup version if expired
	sc.cleanupVersion(version)
}

// LoadSegments loads segments with shardCluster.
// shard cluster shall try to loadSegments in the follower then update the allocation.
func (sc *ShardCluster) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	log := sc.getLogger()
	// add common log fields
	log = log.With(zap.Int64("dstNodeID", req.GetDstNodeID()))

	segmentIDs := make([]int64, 0, len(req.Infos))
	for _, info := range req.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	log = log.With(zap.Int64s("segmentIDs", segmentIDs))

	// notify follower to load segment
	node, ok := sc.getNode(req.GetDstNodeID())
	if !ok {
		log.Warn("node not in cluster")
		return fmt.Errorf("node not in cluster %d", req.GetDstNodeID())
	}

	req = proto.Clone(req).(*querypb.LoadSegmentsRequest)
	req.Base.TargetID = req.GetDstNodeID()
	resp, err := node.client.LoadSegments(ctx, req)
	if err != nil {
		log.Warn("failed to dispatch load segment request", zap.Error(err))
		return err
	}
	if resp.GetErrorCode() == commonpb.ErrorCode_InsufficientMemoryToLoad {
		log.Warn("insufficient memory when follower load segment", zap.String("reason", resp.GetReason()))
		return fmt.Errorf("%w, reason:%s", ErrInsufficientMemory, resp.GetReason())
	}
	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("follower load segment failed", zap.String("reason", resp.GetReason()))
		return fmt.Errorf("follower %d failed to load segment, reason %s", req.DstNodeID, resp.GetReason())
	}
	log.Debug("shardCluster has completed loading segments for:",
		zap.Int64("collectionID:", req.CollectionID), zap.Int64("replicaID:", req.ReplicaID),
		zap.Int64("sourceNodeId", req.SourceNodeID))

	// update allocation
	for _, info := range req.Infos {
		sc.updateSegment(shardSegmentInfo{
			nodeID:      req.DstNodeID,
			segmentID:   info.SegmentID,
			partitionID: info.PartitionID,
			state:       segmentStateLoaded,
			version:     req.GetVersion(),
		})
	}

	// notify handoff wait online if any
	sc.segmentCond.Broadcast()

	sc.mutVersion.Lock()
	defer sc.mutVersion.Unlock()

	// update shardleader allocation view
	allocations := sc.currentVersion.segments.Clone(filterNothing)
	for _, info := range req.Infos {
		allocations[info.SegmentID] = shardSegmentInfo{nodeID: req.DstNodeID, segmentID: info.SegmentID, partitionID: info.PartitionID, state: segmentStateLoaded}
	}

	lastVersion := sc.currentVersion
	version := NewShardClusterVersion(sc.nextVersionID.Inc(), allocations, sc.currentVersion)
	sc.versions.Store(version.versionID, version)
	sc.currentVersion = version
	sc.cleanupVersion(lastVersion)

	return nil
}

// ReleaseSegments releases segments via ShardCluster.
// ShardCluster will wait all on-going search until finished, update the current version,
// then release the segments through follower.
func (sc *ShardCluster) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, force bool) error {
	log := sc.getLogger()
	// add common log fields
	log = log.With(zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.String("scope", req.GetScope().String()),
		zap.Bool("force", force))

	//shardCluster.forceRemoveSegment(action.GetSegmentID())
	offlineSegments := make(typeutil.UniqueSet)
	if req.Scope != querypb.DataScope_Streaming {
		offlineSegments.Insert(req.GetSegmentIDs()...)
	}

	var lastVersion *ShardClusterVersion
	var err error
	func() {
		sc.mutVersion.Lock()
		defer sc.mutVersion.Unlock()

		var allocations SegmentsStatus
		if sc.currentVersion != nil {
			allocations = sc.currentVersion.segments.Clone(func(segmentID UniqueID, nodeID UniqueID) bool {
				return (nodeID == req.NodeID || force) && offlineSegments.Contain(segmentID)
			})
		}

		// generate a new version
		versionID := sc.nextVersionID.Inc()
		// remove offline segments in next version
		// so incoming request will not have allocation of these segments
		version := NewShardClusterVersion(versionID, allocations, sc.currentVersion)
		sc.versions.Store(versionID, version)

		// force release means current distribution has error
		if !force {
			// currentVersion shall be not nil
			if sc.currentVersion != nil {
				// wait for last version search done
				<-sc.currentVersion.Expire()
				lastVersion = sc.currentVersion
			}
		}

		// set current version to new one
		sc.currentVersion = version

		// force release skips the release call
		if force {
			return
		}

		// try to release segments from nodes
		node, ok := sc.getNode(req.GetNodeID())
		if !ok {
			log.Warn("node not in cluster", zap.Int64("nodeID", req.NodeID))
			err = fmt.Errorf("node %d not in cluster ", req.NodeID)
			return
		}

		req = proto.Clone(req).(*querypb.ReleaseSegmentsRequest)
		req.Base.TargetID = req.GetNodeID()
		resp, rerr := node.client.ReleaseSegments(ctx, req)
		if err != nil {
			log.Warn("failed to dispatch release segment request", zap.Error(err))
			err = rerr
			return
		}
		if resp.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("follower release segment failed", zap.String("reason", resp.GetReason()))
			err = fmt.Errorf("follower %d failed to release segment, reason %s", req.NodeID, resp.GetReason())
		}
	}()
	sc.cleanupVersion(lastVersion)

	sc.mut.Lock()
	// do not delete segment if data scope is streaming
	if req.GetScope() != querypb.DataScope_Streaming {
		for _, segmentID := range req.SegmentIDs {
			info, ok := sc.segments[segmentID]
			if ok {
				// otherwise, segment is on another node, do nothing
				if force || info.nodeID == req.NodeID {
					delete(sc.segments, segmentID)
				}
			}
		}
	}
	sc.healthCheck()
	sc.mut.Unlock()

	return err
}

// cleanupVersion clean up version from map
func (sc *ShardCluster) cleanupVersion(version *ShardClusterVersion) {
	// last version nil, just return
	if version == nil {
		return
	}

	// if version is still current one or still in use, return
	if version.current.Load() || version.inUse.Load() > 0 {
		return
	}

	sc.versions.Delete(version.versionID)
}

// waitSegmentsOnline waits until all provided segments is loaded.
func (sc *ShardCluster) waitSegmentsOnline(segments []shardSegmentInfo) {
	sc.segmentCond.L.Lock()
	for !sc.segmentsOnline(segments) {
		sc.segmentCond.Wait()
	}
	sc.segmentCond.L.Unlock()
}

// checkOnline checks whether all segment info provided in online state.
func (sc *ShardCluster) segmentsOnline(segments []shardSegmentInfo) bool {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	for _, segInfo := range segments {
		segment, ok := sc.segments[segInfo.segmentID]
		// check segment online on #specified Node#
		if !ok || segment.state != segmentStateLoaded || segment.nodeID != segInfo.nodeID {
			return false
		}
	}
	return true
}

// GetStatistics returns the statistics on the shard cluster.
func (sc *ShardCluster) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest, withStreaming getStatisticsWithStreaming) ([]*internalpb.GetStatisticsResponse, error) {
	if !sc.serviceable() {
		return nil, fmt.Errorf("ShardCluster for %s replicaID %d is not available", sc.vchannelName, sc.replicaID)
	}
	if !funcutil.SliceContain(req.GetDmlChannels(), sc.vchannelName) {
		return nil, fmt.Errorf("ShardCluster for %s does not match request channels :%v", sc.vchannelName, req.GetDmlChannels())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	log.Debug("cluster segment distribution", zap.Int("len", len(segAllocs)))
	for nodeID, segmentIDs := range segAllocs {
		log.Debug("segments distribution", zap.Int64("nodeID", nodeID), zap.Int64s("segments", segmentIDs))
	}

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var resultMut sync.Mutex
	results := make([]*internalpb.GetStatisticsResponse, 0, len(segAllocs)) // count(nodes) + 1(growing)

	// detect corresponding streaming search is done
	wg.Add(1)
	go func() {
		defer wg.Done()
		if withStreaming == nil {
			return
		}

		streamErr, streamResult := withStreaming(reqCtx)
		resultMut.Lock()
		defer resultMut.Unlock()
		if streamErr != nil {
			cancel()
			// not set cancel error
			if !errors.Is(streamErr, context.Canceled) {
				err = fmt.Errorf("stream operation failed: %w", streamErr)
			}
		}
		if streamResult != nil {
			results = append(results, streamResult)
		}
	}()

	// dispatch request to followers
	for nodeID, segments := range segAllocs {
		nodeReq := &querypb.GetStatisticsRequest{
			Req:             req.GetReq(),
			DmlChannels:     req.GetDmlChannels(),
			FromShardLeader: true,
			Scope:           querypb.DataScope_Historical,
			SegmentIDs:      segments,
		}
		node, ok := sc.getNode(nodeID)
		if !ok { // meta mismatch, report error
			return nil, WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			partialResult, nodeErr := node.client.GetStatistics(reqCtx, nodeReq)
			resultMut.Lock()
			defer resultMut.Unlock()
			if nodeErr != nil || partialResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				cancel()
				// not set cancel error
				if !errors.Is(nodeErr, context.Canceled) {
					err = fmt.Errorf("GetStatistic %d failed, reason %s err %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				}
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return results, nil
}

// Search preforms search operation on shard cluster.
func (sc *ShardCluster) Search(ctx context.Context, req *querypb.SearchRequest, withStreaming searchWithStreaming) ([]*internalpb.SearchResults, error) {
	log := log.Ctx(ctx)
	if !sc.serviceable() {
		err := WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName)
		log.Warn("failed to search on shard",
			zap.Int64("replicaID", sc.replicaID),
			zap.String("channel", sc.vchannelName),
			zap.Int32("state", sc.state.Load()),
			zap.Any("version", sc.currentVersion),
			zap.Error(err),
		)
		return nil, err
	}
	if !funcutil.SliceContain(req.GetDmlChannels(), sc.vchannelName) {
		return nil, fmt.Errorf("ShardCluster for %s does not match request channels :%v", sc.vchannelName, req.GetDmlChannels())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	log.Debug("cluster segment distribution", zap.Int("len", len(segAllocs)), zap.Int64s("partitionIDs", req.GetReq().GetPartitionIDs()))
	for nodeID, segmentIDs := range segAllocs {
		log.Debug("segments distribution", zap.Int64("nodeID", nodeID), zap.Int64s("segments", segmentIDs))
	}

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var resultMut sync.Mutex
	results := make([]*internalpb.SearchResults, 0, len(segAllocs)) // count(nodes) + 1(growing)

	// detect corresponding streaming search is done
	wg.Add(1)
	go func() {
		defer wg.Done()
		if withStreaming == nil {
			return
		}

		streamErr, streamResult := withStreaming(reqCtx)
		resultMut.Lock()
		defer resultMut.Unlock()
		if streamErr != nil {
			if err == nil {
				err = fmt.Errorf("stream operation failed: %w", streamErr)
			}
			cancel()
		}
		if streamResult != nil {
			results = append(results, streamResult)
		}
	}()

	// dispatch request to followers
	for nodeID, segments := range segAllocs {
		internalReq := typeutil.Clone(req.GetReq())
		internalReq.GetBase().TargetID = nodeID
		nodeReq := &querypb.SearchRequest{
			Req:             internalReq,
			DmlChannels:     req.DmlChannels,
			FromShardLeader: true,
			Scope:           querypb.DataScope_Historical,
			SegmentIDs:      segments,
		}
		node, ok := sc.getNode(nodeID)
		if !ok { // meta dismatch, report error
			return nil, fmt.Errorf("%w, node %d not found",
				WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName),
				nodeID,
			)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()

			partialResult, nodeErr := node.client.Search(reqCtx, nodeReq)

			resultMut.Lock()
			defer resultMut.Unlock()
			if nodeErr != nil || partialResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				if err == nil {
					err = fmt.Errorf("Search %d failed, reason %s err %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				}
				cancel()
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if err != nil {
		log.Warn("failed to do search",
			zap.Int64("sourceID", req.GetReq().GetBase().GetSourceID()),
			zap.Strings("channels", req.GetDmlChannels()),
			zap.Int64s("segmentIDs", req.GetSegmentIDs()),
			zap.Error(err))
		return nil, err
	}

	return results, nil
}

// Query performs query operation on shard cluster.
func (sc *ShardCluster) Query(ctx context.Context, req *querypb.QueryRequest, withStreaming queryWithStreaming) ([]*internalpb.RetrieveResults, error) {
	if !sc.serviceable() {
		return nil, WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName)
	}

	// handles only the dml channel part, segment ids is dispatch by cluster itself
	if !funcutil.SliceContain(req.GetDmlChannels(), sc.vchannelName) {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channels :%v", sc.vchannelName, req.GetDmlChannels())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var resultMut sync.Mutex

	results := make([]*internalpb.RetrieveResults, 0, len(segAllocs)+1) // count(nodes) + 1(growing)

	// detect corresponding streaming query is done
	wg.Add(1)
	go func() {
		defer wg.Done()
		if withStreaming == nil {
			return
		}

		streamErr, streamResult := withStreaming(reqCtx)
		resultMut.Lock()
		defer resultMut.Unlock()
		if streamErr != nil {
			if err == nil {
				err = fmt.Errorf("stream operation failed: %w", streamErr)
			}
			cancel()
		}
		if streamResult != nil {
			results = append(results, streamResult)
		}
	}()

	// dispatch request to followers
	for nodeID, segments := range segAllocs {
		internalReq := typeutil.Clone(req.GetReq())
		internalReq.GetBase().TargetID = nodeID
		nodeReq := &querypb.QueryRequest{
			Req:             internalReq,
			FromShardLeader: true,
			SegmentIDs:      segments,
			Scope:           querypb.DataScope_Historical,
			DmlChannels:     req.DmlChannels,
		}
		node, ok := sc.getNode(nodeID)
		if !ok { // meta dismatch, report error
			return nil, WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			partialResult, nodeErr := node.client.Query(reqCtx, nodeReq)
			resultMut.Lock()
			defer resultMut.Unlock()
			if nodeErr != nil || partialResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				err = fmt.Errorf("Query %d failed, reason %s err %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				cancel()
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	return results, nil
}

func (sc *ShardCluster) GetSegmentInfos() []shardSegmentInfo {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	ret := make([]shardSegmentInfo, 0, len(sc.segments))
	for _, info := range sc.segments {
		ret = append(ret, info)
	}
	return ret
}

func (sc *ShardCluster) getVersion() int64 {
	return sc.version
}

func getSearchWithStreamingFunc(searchCtx context.Context, req *querypb.SearchRequest, node *QueryNode, qs *queryShard, nodeID int64) searchWithStreaming {
	return func(ctx context.Context) (error, *internalpb.SearchResults) {
		streamingTask, err := newSearchTask(searchCtx, req)
		if err != nil {
			return err, nil
		}
		streamingTask.QS = qs
		streamingTask.DataScope = querypb.DataScope_Streaming
		err = node.scheduler.AddReadTask(searchCtx, streamingTask)
		if err != nil {
			return err, nil
		}
		err = streamingTask.WaitToFinish()
		if err != nil {
			return err, nil
		}
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(nodeID),
			metrics.SearchLabel).Observe(float64(streamingTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(nodeID),
			metrics.SearchLabel).Observe(float64(streamingTask.reduceDur.Milliseconds()))
		return nil, streamingTask.Ret
	}
}

func getQueryWithStreamingFunc(queryCtx context.Context, req *querypb.QueryRequest, node *QueryNode, qs *queryShard, nodeID int64) queryWithStreaming {
	return func(ctx context.Context) (error, *internalpb.RetrieveResults) {
		streamingTask := newQueryTask(queryCtx, req)
		streamingTask.DataScope = querypb.DataScope_Streaming
		streamingTask.QS = qs
		err := node.scheduler.AddReadTask(queryCtx, streamingTask)

		if err != nil {
			return err, nil
		}
		err = streamingTask.WaitToFinish()
		if err != nil {
			return err, nil
		}
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(nodeID),
			metrics.QueryLabel).Observe(float64(streamingTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(nodeID),
			metrics.QueryLabel).Observe(float64(streamingTask.reduceDur.Milliseconds()))
		return nil, streamingTask.Ret
	}
}
