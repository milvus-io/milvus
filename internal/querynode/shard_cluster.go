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
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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
}

type segmentEvent struct {
	eventType   segmentEventType
	segmentID   int64
	partitionID int64
	nodeID      int64
	state       segmentState
}

type shardQueryNode interface {
	Search(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error)
	Query(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
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

// ShardNodeDetector provides method to detect node events
type ShardNodeDetector interface {
	watchNodes(collectionID int64, replicaID int64, vchannelName string) ([]nodeEvent, <-chan nodeEvent)
}

// ShardSegmentDetector provides method to detect segment events
type ShardSegmentDetector interface {
	watchSegments(collectionID int64, replicaID int64, vchannelName string) ([]segmentEvent, <-chan segmentEvent)
}

// ShardNodeBuilder function type to build types.QueryNode from addr and id
type ShardNodeBuilder func(nodeID int64, addr string) shardQueryNode

// ShardCluster maintains the ShardCluster information and perform shard level operations
type ShardCluster struct {
	state *atomic.Int32

	collectionID int64
	replicaID    int64
	vchannelName string

	nodeDetector    ShardNodeDetector
	segmentDetector ShardSegmentDetector
	nodeBuilder     ShardNodeBuilder

	mut         sync.RWMutex
	nodes       map[int64]*shardNode                 // online nodes
	segments    map[int64]*shardSegmentInfo          // shard segments
	handoffs    map[int32]*querypb.SegmentChangeInfo // current pending handoff
	lastToken   *atomic.Int32                        // last token used for segment change info
	segmentCond *sync.Cond                           // segment state change condition
	rcCond      *sync.Cond                           // segment rc change condition

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

		nodes:     make(map[int64]*shardNode),
		segments:  make(map[int64]*shardSegmentInfo),
		handoffs:  make(map[int32]*querypb.SegmentChangeInfo),
		lastToken: atomic.NewInt32(0),

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
	sc.closeOnce.Do(func() {
		sc.state.Store(int32(unavailable))
		close(sc.closeCh)
	})
}

// addNode add a node into cluster
func (sc *ShardCluster) addNode(evt nodeEvent) {
	log.Debug("ShardCluster add node", zap.Int64("nodeID", evt.nodeID))
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

	sc.nodes[evt.nodeID] = &shardNode{
		nodeID:   evt.nodeID,
		nodeAddr: evt.nodeAddr,
		client:   sc.nodeBuilder(evt.nodeID, evt.nodeAddr),
	}
}

// removeNode handles node offline and setup related segments
func (sc *ShardCluster) removeNode(evt nodeEvent) {
	sc.mut.Lock()
	defer sc.mut.Unlock()

	old, ok := sc.nodes[evt.nodeID]
	if !ok {
		log.Warn("ShardCluster removeNode does not belong to it", zap.Int64("nodeID", evt.nodeID), zap.String("addr", evt.nodeAddr))
		return
	}

	defer old.client.Stop()
	delete(sc.nodes, evt.nodeID)

	for _, segment := range sc.segments {
		if segment.nodeID == evt.nodeID {
			segment.state = segmentStateOffline
			sc.state.Store(int32(unavailable))
		}
	}
}

// updateSegment apply segment change to shard cluster
func (sc *ShardCluster) updateSegment(evt segmentEvent) {
	log.Debug("ShardCluster update segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))

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
		sc.segments[evt.segmentID] = &shardSegmentInfo{
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
	// notify handoff wait online if any
	defer func() {
		sc.segmentCond.L.Lock()
		sc.segmentCond.Broadcast()
		sc.segmentCond.L.Unlock()
	}()
	sc.mut.Lock()
	defer sc.mut.Unlock()

	for _, line := range distribution {
		for _, segmentID := range line.GetSegmentIds() {
			old, ok := sc.segments[segmentID]
			if !ok { // newly add
				sc.segments[segmentID] = &shardSegmentInfo{
					nodeID:      line.GetNodeId(),
					partitionID: line.GetPartitionId(),
					segmentID:   segmentID,
					state:       state,
				}
				continue
			}

			sc.transferSegment(old, segmentEvent{
				eventType:   segmentAdd,
				nodeID:      line.GetNodeId(),
				partitionID: line.GetPartitionId(),
				segmentID:   segmentID,
				state:       state,
			})
		}
	}
}

// transferSegment apply segment state transition.
func (sc *ShardCluster) transferSegment(old *shardSegmentInfo, evt segmentEvent) {
	switch old.state {
	case segmentStateOffline: // safe to update nodeID and state
		old.nodeID = evt.nodeID
		old.state = evt.state
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
		if evt.state == segmentStateLoaded {
			sc.healthCheck()
		}
	case segmentStateLoaded:
		old.nodeID = evt.nodeID
		old.state = evt.state
		if evt.state != segmentStateLoaded {
			sc.healthCheck()
		}
	}
}

// removeSegment removes segment from cluster
// should only applied in hand-off or load balance procedure
func (sc *ShardCluster) removeSegment(evt segmentEvent) {
	log.Debug("ShardCluster remove segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))

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
		sc.updateSegment(segment)
	}
	go sc.watchSegments(segmentEvtCh)

	sc.healthCheck()
}

// healthCheck iterate all segments to to check cluster could provide service.
func (sc *ShardCluster) healthCheck() {
	for _, segment := range sc.segments {
		if segment.state != segmentStateLoaded { // TODO check hand-off or load balance
			sc.state.Store(int32(unavailable))
			return
		}
	}
	sc.state.Store(int32(available))
}

// watchNodes handles node events.
func (sc *ShardCluster) watchNodes(evtCh <-chan nodeEvent) {
	for {
		select {
		case evt, ok := <-evtCh:
			log.Debug("node event", zap.Any("evt", evt))
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
			switch evt.eventType {
			case segmentAdd:
				sc.updateSegment(evt)
			case segmentDel:
				sc.removeSegment(evt)
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
func (sc *ShardCluster) getSegment(segmentID int64) (*shardSegmentInfo, bool) {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	segment, ok := sc.segments[segmentID]
	if !ok {
		return nil, false
	}
	return &shardSegmentInfo{
		segmentID:   segment.segmentID,
		nodeID:      segment.nodeID,
		partitionID: segment.partitionID,
		state:       segment.state,
	}, true
}

// segmentAllocations returns node to segments mappings.
// calling this function also increases the reference count of related segments.
func (sc *ShardCluster) segmentAllocations(partitionIDs []int64) map[int64][]int64 {
	result := make(map[int64][]int64) // nodeID => segmentIDs
	sc.mut.Lock()
	defer sc.mut.Unlock()

	for _, segment := range sc.segments {
		if len(partitionIDs) > 0 && !inList(partitionIDs, segment.partitionID) {
			continue
		}
		if sc.inHandoffOffline(segment.segmentID) {
			log.Debug("segment ignore in pending offline list", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID), zap.Int64("segmentID", segment.segmentID))
			continue
		}
		// reference count ++
		segment.inUse++
		result[segment.nodeID] = append(result[segment.nodeID], segment.segmentID)
	}
	return result
}

// inHandoffOffline checks whether segment is pending handoff offline list
// Note that sc.mut Lock is assumed to be hold outside of this function!
func (sc *ShardCluster) inHandoffOffline(segmentID int64) bool {
	for _, handoff := range sc.handoffs {
		for _, offlineSegment := range handoff.OfflineSegments {
			if segmentID == offlineSegment.GetSegmentID() {
				return true
			}
		}
	}
	return false
}

// finishUsage decreases the inUse count of provided segments
func (sc *ShardCluster) finishUsage(allocs map[int64][]int64) {
	defer func() {
		sc.rcCond.L.Lock()
		sc.rcCond.Broadcast()
		sc.rcCond.L.Unlock()
	}()
	sc.mut.Lock()
	defer sc.mut.Unlock()
	for _, segments := range allocs {
		for _, segmentID := range segments {
			segment, ok := sc.segments[segmentID]
			if !ok || segment == nil {
				// this shall not happen, since removing segment without decreasing rc to zero is illegal
				log.Error("finishUsage with non-existing segment", zap.Int64("collectionID", sc.collectionID), zap.Int64("replicaID", sc.replicaID), zap.String("vchannel", sc.vchannelName), zap.Int64("segmentID", segmentID))
				continue
			}
			// decrease the reference count
			segment.inUse--
		}
	}
}

// HandoffSegments processes the handoff/load balance segments update procedure.
func (sc *ShardCluster) HandoffSegments(info *querypb.SegmentChangeInfo) error {
	// wait for all OnlineSegment is loaded
	onlineSegments := make([]int64, 0, len(info.OnlineSegments))
	for _, seg := range info.OnlineSegments {
		// filter out segments not maintained in this cluster
		if seg.GetCollectionID() != sc.collectionID || seg.GetDmChannel() != sc.vchannelName {
			continue
		}
		onlineSegments = append(onlineSegments, seg.GetSegmentID())
	}
	sc.waitSegmentsOnline(onlineSegments)

	// add segmentChangeInfo to pending list
	token := sc.appendHandoff(info)

	// wait for all OfflineSegments is not in use
	offlineSegments := make([]int64, 0, len(info.OfflineSegments))
	for _, seg := range info.OfflineSegments {
		offlineSegments = append(offlineSegments, seg.GetSegmentID())
	}
	sc.waitSegmentsNotInUse(offlineSegments)
	// remove offline segments record
	for _, seg := range info.OfflineSegments {
		// filter out segments not maintained in this cluster
		if seg.GetCollectionID() != sc.collectionID || seg.GetDmChannel() != sc.vchannelName {
			continue
		}
		sc.removeSegment(segmentEvent{segmentID: seg.GetSegmentID(), nodeID: seg.GetNodeID()})
	}

	// finish handoff and remove it from pending list
	sc.finishHandoff(token)

	return nil
}

// appendHandoff adds the change info into pending list and returns the token.
func (sc *ShardCluster) appendHandoff(info *querypb.SegmentChangeInfo) int32 {
	sc.mut.Lock()
	defer sc.mut.Unlock()

	token := sc.lastToken.Add(1)
	sc.handoffs[token] = info
	return token
}

// finishHandoff removes the handoff related to the token.
func (sc *ShardCluster) finishHandoff(token int32) {
	sc.mut.Lock()
	defer sc.mut.Unlock()

	delete(sc.handoffs, token)
}

// waitSegmentsOnline waits until all provided segments is loaded.
func (sc *ShardCluster) waitSegmentsOnline(segments []int64) {
	sc.segmentCond.L.Lock()
	for !sc.segmentsOnline(segments) {
		sc.segmentCond.Wait()
	}
	sc.segmentCond.L.Unlock()
}

// waitSegmentsNotInUse waits until all provided segments is not in use.
func (sc *ShardCluster) waitSegmentsNotInUse(segments []int64) {
	sc.rcCond.L.Lock()
	for sc.segmentsInUse(segments) {
		sc.rcCond.Wait()
	}
	sc.rcCond.L.Unlock()
}

// checkOnline checks whether all segment ids provided in online state.
func (sc *ShardCluster) segmentsOnline(segmentIDs []int64) bool {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	for _, segID := range segmentIDs {
		segment, ok := sc.segments[segID]
		if !ok || segment.state != segmentStateLoaded {
			return false
		}
	}
	return true
}

// segmentsInUse checks whether all segment ids provided still in use.
func (sc *ShardCluster) segmentsInUse(segmentIDs []int64) bool {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	for _, segID := range segmentIDs {
		segment, ok := sc.segments[segID]
		if !ok {
			// ignore missing segments, since they might be in streaming
			continue
		}
		if segment.inUse > 0 {
			return true
		}
	}
	return false
}

// Search preforms search operation on shard cluster.
func (sc *ShardCluster) Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error) {
	if sc.state.Load() != int32(available) {
		return nil, fmt.Errorf("ShardCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
	}

	if sc.vchannelName != req.GetDmlChannel() {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channel :%s", sc.vchannelName, req.GetDmlChannel())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(segAllocs)

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
	results := make([]*internalpb.SearchResults, 0, len(segAllocs)+1) // count(nodes) + 1(growing)

	for nodeID, segments := range segAllocs {
		nodeReq := proto.Clone(req).(*querypb.SearchRequest)
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
				err = fmt.Errorf("Search %d failed, reason %s err %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Query performs query operation on shard cluster.
func (sc *ShardCluster) Query(ctx context.Context, req *querypb.QueryRequest) ([]*internalpb.RetrieveResults, error) {
	if sc.state.Load() != int32(available) {
		return nil, fmt.Errorf("ShardCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
	}

	// handles only the dml channel part, segment ids is dispatch by cluster itself
	if sc.vchannelName != req.GetDmlChannel() {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channel :%s", sc.vchannelName, req.GetDmlChannel())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(segAllocs)

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var resultMut sync.Mutex
	results := make([]*internalpb.RetrieveResults, 0, len(segAllocs)+1) // count(nodes) + 1(growing)

	for nodeID, segments := range segAllocs {
		nodeReq := proto.Clone(req).(*querypb.QueryRequest)
		nodeReq.SegmentIDs = segments
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
				err = fmt.Errorf("Query %d failed, reason %s err %w", node.nodeID, partialResult.GetStatus().GetReason(), nodeErr)
				return
			}
			results = append(results, partialResult)
		}()
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}

	return results, nil
}
