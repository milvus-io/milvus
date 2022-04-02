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
	eventType segmentEventType
	segmentID int64
	nodeID    int64
	state     segmentState
}

type shardQueryNode interface {
	Search(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error)
	Query(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	Stop()
}

type shardNode struct {
	nodeID   int64
	nodeAddr string
	client   shardQueryNode
}

type shardSegmentInfo struct {
	segmentID int64
	nodeID    int64
	state     segmentState
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

	mut      sync.RWMutex
	nodes    map[int64]*shardNode        // online nodes
	segments map[int64]*shardSegmentInfo // shard segments

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

		nodes:    make(map[int64]*shardNode),
		segments: make(map[int64]*shardSegmentInfo),

		closeCh: make(chan struct{}),
	}

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
	sc.mut.Lock()
	defer sc.mut.Unlock()

	old, ok := sc.segments[evt.segmentID]
	if !ok { // newly add
		sc.segments[evt.segmentID] = &shardSegmentInfo{
			nodeID:    evt.nodeID,
			segmentID: evt.segmentID,
			state:     evt.state,
		}
		return
	}

	sc.transferSegment(old, evt)
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

	//TODO check handoff / load balance
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
		segmentID: segment.segmentID,
		nodeID:    segment.nodeID,
		state:     segment.state,
	}, true
}

// segmentAllocations returns node to segments mappings.
func (sc *ShardCluster) segmentAllocations() map[int64][]int64 {
	result := make(map[int64][]int64) // nodeID => segmentIDs
	sc.mut.RLock()
	defer sc.mut.RUnlock()

	for _, segment := range sc.segments {
		result[segment.nodeID] = append(result[segment.nodeID], segment.segmentID)
	}
	return result
}

// Search preforms search operation on shard cluster.
func (sc *ShardCluster) Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error) {
	if sc.state.Load() != int32(available) {
		return nil, fmt.Errorf("SharcCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
	}

	if sc.vchannelName != req.GetDmlChannel() {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channel :%s", sc.vchannelName, req.GetDmlChannel())
	}

	// get node allocation
	segAllocs := sc.segmentAllocations()

	// TODO dispatch to local queryShardService query dml channel growing segments

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var resultMut sync.Mutex
	results := make([]*internalpb.SearchResults, 0, len(segAllocs)+1) // count(nodes) + 1(growing)

	for nodeID, segments := range segAllocs {
		nodeReq := proto.Clone(req).(*querypb.SearchRequest)
		nodeReq.DmlChannel = ""
		nodeReq.SegmentIDs = segments
		node, ok := sc.getNode(nodeID)
		if !ok { // meta dismatch, report error
			return nil, fmt.Errorf("SharcCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
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
		return nil, fmt.Errorf("SharcCluster for %s replicaID %d is no available", sc.vchannelName, sc.replicaID)
	}

	// handles only the dml channel part, segment ids is dispatch by cluster itself
	if sc.vchannelName != req.GetDmlChannel() {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channel :%s", sc.vchannelName, req.GetDmlChannel())
	}

	// get node allocation
	segAllocs := sc.segmentAllocations()

	// TODO dispatch to local queryShardService query dml channel growing segments

	// concurrent visiting nodes
	var wg sync.WaitGroup
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	var resultMut sync.Mutex
	results := make([]*internalpb.RetrieveResults, 0, len(segAllocs)+1) // count(nodes) + 1(growing)

	for nodeID, segments := range segAllocs {
		nodeReq := proto.Clone(req).(*querypb.QueryRequest)
		nodeReq.DmlChannel = ""
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
