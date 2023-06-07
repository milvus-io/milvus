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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/contextutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
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
type (
	searchWithStreaming        func(ctx context.Context) (error, *internalpb.SearchResults)
	queryWithStreaming         func(ctx context.Context) (error, *internalpb.RetrieveResults)
	getStatisticsWithStreaming func(ctx context.Context) (error, *internalpb.GetStatisticsResponse)
)

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

	mut          sync.RWMutex
	leader       *shardNode                                 // shard leader node instance
	nodes        *typeutil.ConcurrentMap[int64, *shardNode] // online nodes
	mutVersion   sync.RWMutex
	distribution *distribution

	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewShardCluster create a ShardCluster with provided information.
func NewShardCluster(collectionID int64, replicaID int64, vchannelName string, version int64,
	nodeDetector ShardNodeDetector, segmentDetector ShardSegmentDetector, nodeBuilder ShardNodeBuilder,
) *ShardCluster {
	sc := &ShardCluster{
		state: atomic.NewInt32(int32(unavailable)),

		collectionID: collectionID,
		replicaID:    replicaID,
		vchannelName: vchannelName,
		version:      version,

		segmentDetector: segmentDetector,
		nodeDetector:    nodeDetector,
		nodeBuilder:     nodeBuilder,
		nodes:           typeutil.NewConcurrentMap[int64, *shardNode](),

		closeCh: make(chan struct{}),
	}

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

// serviceable returns whether shard cluster could provide query service, used only for test
func (sc *ShardCluster) serviceable() bool {
	sc.mutVersion.RLock()
	defer sc.mutVersion.RUnlock()
	return sc.distribution != nil && sc.distribution.Serviceable()
}

// addNode add a node into cluster
func (sc *ShardCluster) addNode(evt nodeEvent) {
	log := sc.getLogger()
	log.Info("ShardCluster add node", zap.Int64("nodeID", evt.nodeID))
	sc.mut.Lock()
	defer sc.mut.Unlock()

	oldNode, ok := sc.nodes.Get(evt.nodeID)
	if ok {
		if oldNode.nodeAddr == evt.nodeAddr {
			log.Warn("ShardCluster add same node, skip", zap.Int64("nodeID", evt.nodeID), zap.String("addr", evt.nodeAddr))
			return
		}
		sc.nodes.GetAndRemove(evt.nodeID)
		defer oldNode.client.Stop()
	}

	node := &shardNode{
		nodeID:   evt.nodeID,
		nodeAddr: evt.nodeAddr,
		client:   sc.nodeBuilder(evt.nodeID, evt.nodeAddr),
	}
	sc.nodes.InsertIfNotPresent(evt.nodeID, node)
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

	old, ok := sc.nodes.GetAndRemove(evt.nodeID)
	if !ok {
		log.Warn("ShardCluster removeNode does not belong to it", zap.Int64("nodeID", evt.nodeID), zap.String("addr", evt.nodeAddr))
		return
	}

	defer old.client.Stop()
	sc.distribution.NodeDown(evt.nodeID)
}

// updateSegment apply segment change to shard cluster
func (sc *ShardCluster) updateSegment(evts ...shardSegmentInfo) {
	log := sc.getLogger()
	for _, evt := range evts {
		log.Info("ShardCluster update segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))
	}

	sc.distribution.UpdateDistribution(lo.Map(evts, func(evt shardSegmentInfo, _ int) SegmentEntry {
		return SegmentEntry{
			SegmentID:   evt.segmentID,
			PartitionID: evt.partitionID,
			NodeID:      evt.nodeID,
			State:       evt.state,
			Version:     evt.version,
		}
	})...)
}

// SetupFirstVersion initialized first version for shard cluster.
func (sc *ShardCluster) SetupFirstVersion() {
	sc.mutVersion.Lock()
	sc.distribution = NewDistribution(sc.replicaID)
	sc.mutVersion.Unlock()
	sc.init()
	sc.updateShardClusterState(available)
}

// SyncSegments synchronize segment distribution in batch
func (sc *ShardCluster) SyncSegments(info []*querypb.ReplicaSegmentsInfo, state segmentState) {
	log := sc.getLogger()
	log.Info("ShardCluster sync segments", zap.Any("replica segments", info), zap.Int32("state", int32(state)))

	var d *distribution
	sc.mutVersion.RLock()
	d = sc.distribution
	sc.mutVersion.RUnlock()
	if d == nil {
		log.Warn("received SyncSegments call before version setup")
		return
	}

	entries := make([]SegmentEntry, 0, len(info))
	for _, line := range info {
		for i, segmentID := range line.GetSegmentIds() {
			nodeID := line.GetNodeId()
			version := line.GetVersions()[i]
			// if node id not in replica node list, this line shall be placeholder for segment offline
			_, ok := sc.getNode(nodeID)
			if !ok {
				log.Warn("Sync segment with invalid nodeID", zap.Int64("segmentID", segmentID), zap.Int64("nodeID", line.NodeId))
				nodeID = common.InvalidNodeID
			}

			entries = append(entries, SegmentEntry{
				NodeID:      nodeID,
				PartitionID: line.GetPartitionId(),
				SegmentID:   segmentID,
				State:       state,
				Version:     version,
			})
		}
	}
	sc.distribution.UpdateDistribution(entries...)
}

// removeSegment removes segment from cluster
// should only applied in hand-off or load balance procedure
func (sc *ShardCluster) removeSegment(evt shardSegmentInfo) {
	log := sc.getLogger()
	log.Info("ShardCluster remove segment", zap.Int64("nodeID", evt.nodeID), zap.Int64("segmentID", evt.segmentID), zap.Int32("state", int32(evt.state)))

	sc.distribution.RemoveDistributions(func() {}, SegmentEntry{
		SegmentID: evt.segmentID,
		NodeID:    evt.nodeID,
	})
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
		if !ok {
			log.Warn("init with invalid node id", zap.Int64("segmentID", segment.segmentID))
		}
		sc.updateSegment(info)
	}
	go sc.watchSegments(segmentEvtCh)
}

// pickNode selects node in the cluster
func (sc *ShardCluster) pickNode(evt segmentEvent) (shardSegmentInfo, bool) {
	info := shardSegmentInfo{
		segmentID:   evt.segmentID,
		partitionID: evt.partitionID,
		nodeID:      common.InvalidNodeID,
		state:       evt.state,
	}
	nodeID, has := sc.selectNodeInReplica(evt.nodeIDs)
	if has { // assume one segment shall exist once in one replica
		info.nodeID = nodeID
		return info, true
	}

	return info, false
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
	node, ok := sc.nodes.Get(nodeID)
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
	sc.distribution.mut.RLock()
	defer sc.distribution.mut.RUnlock()
	segment, ok := sc.distribution.sealedSegments[segmentID]
	return shardSegmentInfo{
		segmentID:   segment.SegmentID,
		partitionID: segment.PartitionID,
		nodeID:      segment.NodeID,
		state:       segment.State,
		version:     segment.Version,
	}, ok
}

// segmentAllocations returns node to segments mappings.
// calling this function also increases the reference count of related segments.
func (sc *ShardCluster) segmentAllocations(partitionIDs []int64) (map[int64][]int64, int64) {
	sc.mutVersion.RLock()
	defer sc.mutVersion.RUnlock()
	if sc.distribution == nil {
		return nil, -1
	}
	items, version := sc.distribution.GetCurrent(partitionIDs...)
	return lo.SliceToMap(items, func(item SnapshotItem) (int64, []int64) {
		return item.NodeID, lo.Map(item.Segments, func(entry SegmentEntry, _ int) int64 { return entry.SegmentID })
	}), version
}

// finishUsage decreases the inUse count of provided segments
func (sc *ShardCluster) finishUsage(versionID int64) {
	if versionID == -1 {
		return
	}
	sc.distribution.FinishUsage(versionID)
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

	var err error
	var entries []SegmentEntry
	if req.Scope != querypb.DataScope_Streaming {
		entries = lo.Map(req.GetSegmentIDs(), func(segmentID int64, _ int) SegmentEntry {
			nodeID := req.GetNodeID()
			if force {
				nodeID = wildcardNodeID
			}
			return SegmentEntry{
				SegmentID: segmentID,
				NodeID:    nodeID,
			}
		})
	}

	// release operation,
	// requires sc.mut read lock held
	releaseFn := func() {
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
		if rerr != nil {
			log.Warn("failed to dispatch release segment request", zap.Error(rerr))
			err = rerr
			return
		}
		if resp.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("follower release segment failed", zap.String("reason", resp.GetReason()))
			err = fmt.Errorf("follower %d failed to release segment, reason %s", req.NodeID, resp.GetReason())
		}
	}

	sc.mut.RLock()
	defer sc.mut.RUnlock()
	sc.distribution.RemoveDistributions(releaseFn, entries...)

	return err
}

// GetStatistics returns the statistics on the shard cluster.
func (sc *ShardCluster) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest, withStreaming getStatisticsWithStreaming) ([]*internalpb.GetStatisticsResponse, error) {
	if !funcutil.SliceContain(req.GetDmlChannels(), sc.vchannelName) {
		return nil, fmt.Errorf("ShardCluster for %s does not match request channels :%v", sc.vchannelName, req.GetDmlChannels())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	if versionID == -1 {
		return nil, fmt.Errorf("ShardCluster for %s replicaID %d is not available", sc.vchannelName, sc.replicaID)
	}

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
	if !funcutil.SliceContain(req.GetDmlChannels(), sc.vchannelName) {
		return nil, fmt.Errorf("ShardCluster for %s does not match request channels :%v", sc.vchannelName, req.GetDmlChannels())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	// not serviceable
	if versionID == -1 {
		err := WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName)
		log.Warn("failed to search on shard",
			zap.Int64("replicaID", sc.replicaID),
			zap.String("channel", sc.vchannelName),
			zap.Error(err),
		)
		return nil, err
	}

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
		if len(segments) == 0 {
			continue
		}
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
			zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
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
	// handles only the dml channel part, segment ids is dispatch by cluster itself
	if !funcutil.SliceContain(req.GetDmlChannels(), sc.vchannelName) {
		return nil, fmt.Errorf("ShardCluster for %s does not match to request channels :%v", sc.vchannelName, req.GetDmlChannels())
	}

	// get node allocation and maintains the inUse reference count
	segAllocs, versionID := sc.segmentAllocations(req.GetReq().GetPartitionIDs())
	defer sc.finishUsage(versionID)

	// not serviceable
	if versionID == -1 {
		return nil, WrapErrShardNotAvailable(sc.replicaID, sc.vchannelName)
	}

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
		if len(segments) == 0 {
			continue
		}
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

func (sc *ShardCluster) GetSegmentInfos() []SegmentEntry {
	sc.mutVersion.RLock()
	distribution := sc.distribution
	sc.mutVersion.RUnlock()
	// before setup version
	if distribution == nil {
		return nil
	}
	items := distribution.Peek()

	var result []SegmentEntry
	for _, item := range items {
		result = append(result, item.Segments...)
	}
	return result
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
		// In queue latency per user.
		metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
			fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel,
			contextutil.GetUserFromGrpcMetadata(streamingTask.Ctx()),
		).Observe(float64(streamingTask.queueDur.Milliseconds()))

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

		// In queue latency per user.
		metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
			fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel,
			contextutil.GetUserFromGrpcMetadata(streamingTask.Ctx()),
		).Observe(float64(streamingTask.queueDur.Milliseconds()))
		return nil, streamingTask.Ret
	}
}
