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
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/golang/protobuf/proto"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/errorutil"
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
	Search(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error)
	Query(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
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
type withStreaming func(ctx context.Context) error

// ShardCluster maintains the ShardCluster information and perform shard level operations
type ShardCluster struct {
	state *atomic.Int32

	collectionID int64
	replicaID    int64
	vchannelName string

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
	rcCond         *sync.Cond // segment rc change condition

	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewShardCluster create a ShardCluster with provided information.
func NewShardCluster(collectionID int64, replicaID int64, vchannelName string,
	nodeDetector ShardNodeDetector, segmentDetector ShardSegmentDetector, nodeBuilder ShardNodeBuilder) *ShardCluster {
	sc := &ShardCluster{
		state: atomic.NewInt32(int32(unavailable)),

		collectionID: collectionID,
		replicaID:    replicaID,
		vchannelName: vchannelName,

		nodeDetector:    nodeDetector,
		segmentDetector: segmentDetector,
		nodeBuilder:     nodeBuilder,

		nodes:         make(map[int64]*shardNode),
		segments:      make(map[int64]shardSegmentInfo),
		nextVersionID: atomic.NewInt64(0),

		closeCh: make(chan struct{}),
	}

	m := sync.Mutex{}
	sc.segmentCond = sync.NewCond(&m)
	m2 := sync.Mutex{}
	sc.rcCond = sync.NewCond(&m2)

	sc.init()

	return sc
}

func (sc *ShardCluster) Close() {
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
			sc.segments[id] = segment
			sc.updateShardClusterState(unavailable)
		}
	}
	// ignore leader process here
}

// updateSegment apply segment change to shard cluster
func (sc *ShardCluster) updateSegment(evt shardSegmentInfo) {
	log.Info("ShardCluster update segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))
	// notify handoff wait online if any
	defer func() {
		sc.segmentCond.L.Lock()
		sc.segmentCond.Broadcast()
		sc.segmentCond.L.Unlock()
	}()

	sc.mut.Lock()
	defer sc.mut.Unlock()

	old, ok := sc.segments[evt.segmentID]
	if !ok { // newly add
		sc.segments[evt.segmentID] = shardSegmentInfo{
			nodeID:      evt.nodeID,
			partitionID: evt.partitionID,
			segmentID:   evt.segmentID,
			state:       evt.state,
		}
		return
	}

	sc.transferSegment(old, evt)
}

// SyncSegments synchronize segment distribution in batch
func (sc *ShardCluster) SyncSegments(distribution []*querypb.ReplicaSegmentsInfo, state segmentState) {
	log := log.With(zap.Int64("collectionID", sc.collectionID), zap.String("vchannel", sc.vchannelName), zap.Int64("replicaID", sc.replicaID))
	log.Info("ShardCluster sync segments", zap.Any("replica segments", distribution), zap.Int32("state", int32(state)))

	sc.mut.Lock()
	for _, line := range distribution {
		for _, segmentID := range line.GetSegmentIds() {
			nodeID := line.GetNodeId()
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
				}
				continue
			}

			sc.transferSegment(old, shardSegmentInfo{
				nodeID:      nodeID,
				partitionID: line.GetPartitionId(),
				segmentID:   segmentID,
				state:       state,
			})
		}
	}

	allocations := sc.segments.Clone(filterNothing)
	sc.mut.Unlock()

	// notify handoff wait online if any
	sc.segmentCond.L.Lock()
	sc.segmentCond.Broadcast()
	sc.segmentCond.L.Unlock()

	sc.mutVersion.Lock()
	defer sc.mutVersion.Unlock()
	version := NewShardClusterVersion(sc.nextVersionID.Inc(), allocations)
	sc.versions.Store(version.versionID, version)
	sc.currentVersion = version
}

// transferSegment apply segment state transition.
// old\new | Offline | Loading | Loaded
// Offline | OK		 | OK	   | OK
// Loading | OK		 | OK	   | NodeID check
// Loaded  | OK      | OK	   | legacy pending
func (sc *ShardCluster) transferSegment(old shardSegmentInfo, evt shardSegmentInfo) {
	switch old.state {
	case segmentStateOffline: // safe to update nodeID and state
		old.nodeID = evt.nodeID
		old.state = evt.state
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
		sc.segments[old.segmentID] = old
		if evt.state == segmentStateLoaded {
			sc.healthCheck()
		}
	case segmentStateLoaded:
		// load balance
		old.nodeID = evt.nodeID
		old.state = evt.state
		sc.segments[old.segmentID] = old
		if evt.state != segmentStateLoaded {
			sc.healthCheck()
		}
	}
}

// removeSegment removes segment from cluster
// should only applied in hand-off or load balance procedure
func (sc *ShardCluster) removeSegment(evt shardSegmentInfo) {
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
	old := sc.state.Load()
	sc.state.Store(int32(state))

	pc, _, _, _ := runtime.Caller(1)
	callerName := runtime.FuncForPC(pc).Name()

	log.Info("Shard Cluster update state", zap.Int64("collectionID", sc.collectionID),
		zap.Int64("replicaID", sc.replicaID), zap.String("channel", sc.vchannelName),
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
	for {
		select {
		case evt, ok := <-evtCh:
			if !ok {
				log.Warn("ShardCluster node channel closed", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID))
				return
			}
			switch evt.eventType {
			case nodeAdd:
				sc.addNode(evt)
			case nodeDel:
				sc.removeNode(evt)
			}
		case <-sc.closeCh:
			log.Info("ShardCluster watchNode quit", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID), zap.String("vchannelName", sc.vchannelName))
			return
		}
	}
}

// watchSegments handles segment events.
func (sc *ShardCluster) watchSegments(evtCh <-chan segmentEvent) {
	for {
		select {
		case evt, ok := <-evtCh:
			if !ok {
				log.Warn("ShardCluster segment channel closed", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID))
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
			log.Info("ShardCluster watchSegments quit", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID), zap.String("vchannelName", sc.vchannelName))
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
	defer func() {
		sc.rcCond.L.Lock()
		sc.rcCond.Broadcast()
		sc.rcCond.L.Unlock()
	}()

	v, ok := sc.versions.Load(versionID)
	if ok {
		version := v.(*ShardClusterVersion)
		version.FinishUsage()
	}
}

// HandoffSegments processes the handoff/load balance segments update procedure.
func (sc *ShardCluster) HandoffSegments(info *querypb.SegmentChangeInfo) error {
	// wait for all OnlineSegment is loaded
	onlineSegmentIDs := make([]int64, 0, len(info.OnlineSegments))
	onlineSegments := make([]shardSegmentInfo, 0, len(info.OnlineSegments))
	for _, seg := range info.OnlineSegments {
		// filter out segments not maintained in this cluster
		if seg.GetCollectionID() != sc.collectionID || seg.GetDmChannel() != sc.vchannelName {
			continue
		}
		nodeID, has := sc.selectNodeInReplica(seg.NodeIds)
		if !has {
			continue
		}
		onlineSegments = append(onlineSegments, shardSegmentInfo{
			nodeID:    nodeID,
			segmentID: seg.GetSegmentID(),
		})
		onlineSegmentIDs = append(onlineSegmentIDs, seg.GetSegmentID())
	}
	sc.waitSegmentsOnline(onlineSegments)

	// now online segment can provide service, generate a new version
	// add segmentChangeInfo to pending list
	versionID := sc.applySegmentChange(info, onlineSegmentIDs)

	removes := make(map[int64][]int64) // nodeID => []segmentIDs
	// remove offline segments record
	for _, seg := range info.OfflineSegments {
		// filter out segments not maintained in this cluster
		if seg.GetCollectionID() != sc.collectionID || seg.GetDmChannel() != sc.vchannelName {
			continue
		}
		nodeID, has := sc.selectNodeInReplica(seg.NodeIds)
		if !has {
			// remove segment placeholder
			nodeID = common.InvalidNodeID
		}
		sc.removeSegment(shardSegmentInfo{segmentID: seg.GetSegmentID(), nodeID: nodeID})

		// only add remove operations when node is valid
		if nodeID != common.InvalidNodeID {
			removes[nodeID] = append(removes[nodeID], seg.SegmentID)
		}
	}

	var errs errorutil.ErrorList

	// notify querynode(s) to release segments
	for nodeID, segmentIDs := range removes {
		node, ok := sc.getNode(nodeID)
		if !ok {
			log.Warn("node not in cluster", zap.Int64("nodeID", nodeID), zap.Int64("collectionID", sc.collectionID), zap.String("vchannel", sc.vchannelName))
			errs = append(errs, fmt.Errorf("node not in cluster nodeID %d", nodeID))
			continue
		}
		state, err := node.client.ReleaseSegments(context.Background(), &querypb.ReleaseSegmentsRequest{
			CollectionID: sc.collectionID,
			SegmentIDs:   segmentIDs,
			Scope:        querypb.DataScope_Historical,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if state.GetErrorCode() != commonpb.ErrorCode_Success {
			errs = append(errs, fmt.Errorf("Release segments failed with reason: %s", state.GetReason()))
		}
	}

	sc.cleanupVersion(versionID)

	// return err if release fail, however the whole segment change is completed
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// appendHandoff adds the change info into pending list and returns the token.
func (sc *ShardCluster) applySegmentChange(info *querypb.SegmentChangeInfo, onlineSegmentIDs []UniqueID) int64 {

	// the suspects growing segment ids
	// first all online segment shall be tried, for flush-handoff only puts segment in onlineSegments
	// and we need to try all offlineSegments in case flush-compact-handoff case
	possibleGrowingToRemove := make([]UniqueID, 0, len(info.OfflineSegments)+len(onlineSegmentIDs))
	offlineSegments := typeutil.UniqueSet{} // map stores offline segment id for quick check
	for _, offline := range info.OfflineSegments {
		offlineSegments.Insert(offline.GetSegmentID())
		possibleGrowingToRemove = append(possibleGrowingToRemove, offline.GetSegmentID())
	}
	// add online segment ids to suspect list
	possibleGrowingToRemove = append(possibleGrowingToRemove, onlineSegmentIDs...)

	// generate next version allocation
	sc.mut.RLock()
	allocations := sc.segments.Clone(func(segmentID int64) bool {
		return offlineSegments.Contain(segmentID)
	})
	sc.mut.RUnlock()

	sc.mutVersion.Lock()
	defer sc.mutVersion.Unlock()

	// generate a new version
	versionID := sc.nextVersionID.Inc()
	// remove offline segments in next version
	// so incoming request will not have allocation of these segments
	version := NewShardClusterVersion(versionID, allocations)
	sc.versions.Store(versionID, version)

	var lastVersionID int64
	/*
		----------------------------------------------------------------------------
		T0		|T1(S2 online)| T2(change version)|T3(remove G2)|
		----------------------------------------------------------------------------
		G2, G3  |G2, G3		  | G2, G3			  | G3
		----------------------------------------------------------------------------
		S1      |S1, S2		  | S1, S2			  | S1,S2
		----------------------------------------------------------------------------
		v0=[S1] |v0=[S1]	  | v1=[S1,S2]		  | v1=[S1,S2]

		There is no method to ensure search after T2 does not search G2 so that it
		could be removed safely
		Currently, the only safe method is to block incoming allocation, so there is no
		search will be dispatch to G2.
		After shard cluster is able to maintain growing semgents, this version change could
		reduce the lock range
	*/
	// currentVersion shall be not nil
	if sc.currentVersion != nil {
		// wait for last version search done
		<-sc.currentVersion.Expire()
		lastVersionID = sc.currentVersion.versionID
		// remove growing segments if any
		// handles the case for Growing to Sealed Handoff(which does not has offline segment info)
		if sc.leader != nil {
			// error ignored here
			sc.leader.client.ReleaseSegments(context.Background(), &querypb.ReleaseSegmentsRequest{
				CollectionID: sc.collectionID,
				SegmentIDs:   possibleGrowingToRemove,
				Scope:        querypb.DataScope_Streaming,
			})
		}
	}

	// set current version to new one
	sc.currentVersion = version

	return lastVersionID
}

// cleanupVersion clean up version from map
func (sc *ShardCluster) cleanupVersion(versionID int64) {
	sc.mutVersion.RLock()
	defer sc.mutVersion.RUnlock()
	// prevent clean up current version
	if sc.currentVersion != nil && sc.currentVersion.versionID == versionID {
		return
	}
	sc.versions.Delete(versionID)
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

// Search preforms search operation on shard cluster.
func (sc *ShardCluster) Search(ctx context.Context, req *querypb.SearchRequest, withStreaming withStreaming) ([]*internalpb.SearchResults, error) {
	if !sc.serviceable() {
		return nil, fmt.Errorf("ShardCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
	}

	if sc.vchannelName != req.GetDmlChannel() {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channel :%s", sc.vchannelName, req.GetDmlChannel())
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
	results := make([]*internalpb.SearchResults, 0, len(segAllocs)) // count(nodes) + 1(growing)

	// detect corresponding streaming search is done
	wg.Add(1)
	go func() {
		defer wg.Done()

		streamErr := withStreaming(reqCtx)
		resultMut.Lock()
		defer resultMut.Unlock()
		if streamErr != nil {
			cancel()
			// not set cancel error
			if !errors.Is(streamErr, context.Canceled) {
				err = fmt.Errorf("stream operation failed: %w", streamErr)
			}
		}
	}()

	// dispatch request to followers
	for nodeID, segments := range segAllocs {
		nodeReq := proto.Clone(req).(*querypb.SearchRequest)
		nodeReq.FromShardLeader = true
		nodeReq.Scope = querypb.DataScope_Historical
		nodeReq.SegmentIDs = segments
		node, ok := sc.getNode(nodeID)
		if !ok { // meta dismatch, report error
			return nil, fmt.Errorf("ShardCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			partialResult, nodeErr := node.client.Search(reqCtx, nodeReq)
			resultMut.Lock()
			defer resultMut.Unlock()
			if nodeErr != nil || partialResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				cancel()
				// not set cancel error
				if !errors.Is(nodeErr, context.Canceled) {
					err = fmt.Errorf("Search %d failed, reason %s err %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				}
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if err != nil {
		log.Error("failed to do search", zap.Any("req", req), zap.Error(err))
		return nil, err
	}

	return results, nil
}

// Query performs query operation on shard cluster.
func (sc *ShardCluster) Query(ctx context.Context, req *querypb.QueryRequest, withStreaming withStreaming) ([]*internalpb.RetrieveResults, error) {
	if !sc.serviceable() {
		return nil, fmt.Errorf("ShardCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
	}

	// handles only the dml channel part, segment ids is dispatch by cluster itself
	if sc.vchannelName != req.GetDmlChannel() {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channel :%s", sc.vchannelName, req.GetDmlChannel())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var streamErr error
	var historicalErr error
	var resultMut sync.Mutex
	results := make([]*internalpb.RetrieveResults, 0, len(segAllocs)+1) // count(nodes) + 1(growing)

	// detect corresponding streaming query is done
	wg.Add(1)
	go func() {
		defer wg.Done()

		streamErr = withStreaming(reqCtx)
		resultMut.Lock()
		defer resultMut.Unlock()
		if streamErr != nil {
			cancel()
			// not set cancel error
			if !errors.Is(streamErr, context.Canceled) {
				streamErr = fmt.Errorf("stream query failed on nodeID %d, err = %w", Params.QueryNodeCfg.GetNodeID(), streamErr)
				log.Error(streamErr.Error())
			}
		}
	}()

	// dispatch request to followers
	for nodeID, segments := range segAllocs {
		nodeReq := proto.Clone(req).(*querypb.QueryRequest)
		nodeReq.FromShardLeader = true
		nodeReq.SegmentIDs = segments
		nodeReq.Scope = querypb.DataScope_Historical
		node, ok := sc.getNode(nodeID)
		if !ok { // meta dismatch, report error
			return nil, fmt.Errorf("SharcCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			partialResult, nodeErr := node.client.Query(reqCtx, nodeReq)
			resultMut.Lock()
			defer resultMut.Unlock()
			if nodeErr != nil || partialResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				cancel()
				historicalErr = fmt.Errorf("historical query failed on nodeID %d, reason = %s, err = %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				log.Error(historicalErr.Error())
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if streamErr != nil && historicalErr == nil {
		return nil, streamErr
	}

	if streamErr == nil && historicalErr != nil {
		return nil, historicalErr
	}

	if streamErr != nil && historicalErr != nil {
		err := errors.New(streamErr.Error() + ", " + historicalErr.Error())
		return nil, err
	}

	return results, nil
}
