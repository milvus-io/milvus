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

package balance

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

const maxPlacementSnapshotAttempts = 3

type PlacementSnapshotBuilder struct {
	meta      *meta.Meta
	dist      *meta.DistributionManager
	target    meta.TargetManagerInterface
	nodes     *session.NodeManager
	inspector task.BalanceTaskInspector

	buildHook     func(attempt int)
	retryObserver func(resourceGroup string)
}

type PlacementSnapshotBuilderOption func(*PlacementSnapshotBuilder)

func WithSnapshotRetryObserver(observer func(resourceGroup string)) PlacementSnapshotBuilderOption {
	return func(builder *PlacementSnapshotBuilder) {
		builder.retryObserver = observer
	}
}

func NewPlacementSnapshotBuilder(
	metadata *meta.Meta,
	distribution *meta.DistributionManager,
	target meta.TargetManagerInterface,
	nodes *session.NodeManager,
	inspector task.BalanceTaskInspector,
	opts ...PlacementSnapshotBuilderOption,
) *PlacementSnapshotBuilder {
	builder := &PlacementSnapshotBuilder{
		meta:      metadata,
		dist:      distribution,
		target:    target,
		nodes:     nodes,
		inspector: inspector,
	}
	for _, opt := range opts {
		opt(builder)
	}
	return builder
}

func (b *PlacementSnapshotBuilder) Build(
	ctx context.Context,
	resourceGroup string,
	eligibleReplicaIDs []int64,
	carryOver []task.PendingBalanceTaskSnapshot,
) (*PlacementSnapshot, error) {
	for attempt := 0; attempt < maxPlacementSnapshotAttempts; attempt++ {
		if attempt > 0 && b.retryObserver != nil {
			b.retryObserver(resourceGroup)
		}
		distribution := b.dist.Capture()
		pending := b.capturePending()
		snapshot, stable, err := b.buildFromCapture(ctx, resourceGroup, eligibleReplicaIDs, distribution, pending, carryOver)
		if err != nil {
			return nil, err
		}
		if !stable {
			continue
		}
		if b.buildHook != nil {
			b.buildHook(attempt)
		}

		afterDistribution := b.dist.Capture()
		afterPending := b.capturePending()
		after, stable, err := b.captureToken(ctx, resourceGroup, afterDistribution, afterPending)
		if err != nil {
			return nil, err
		}
		if !stable {
			continue
		}
		if snapshot.Token.Equal(after) {
			return snapshot, nil
		}
	}
	return nil, errors.New("placement snapshot changed during all capture attempts")
}

func (b *PlacementSnapshotBuilder) Validate(token AdmissionToken) task.BalanceAdmissionReason {
	if b.meta.ResourceManager.GetResourceGroup(context.Background(), token.Snapshot.ResourceGroup) == nil {
		return task.BalanceAdmissionRGChanged
	}
	distribution := b.dist.Capture()
	pending := b.capturePending()
	current, stable, err := b.captureToken(context.Background(), token.Snapshot.ResourceGroup, distribution, pending)
	if err != nil {
		return task.BalanceAdmissionInternalError
	}
	if !stable {
		return task.BalanceAdmissionTargetChanged
	}
	if current.RGHash != token.Snapshot.RGHash {
		return task.BalanceAdmissionRGChanged
	}
	if current.NodeHash != token.Snapshot.NodeHash {
		return task.BalanceAdmissionNodeIneligible
	}
	if current.ReplicaHash != token.Snapshot.ReplicaHash {
		return task.BalanceAdmissionReplicaChanged
	}
	if current.CurrentTargetVersion[token.CollectionID] != token.Snapshot.CurrentTargetVersion[token.CollectionID] ||
		current.NextTargetVersion[token.CollectionID] != token.Snapshot.NextTargetVersion[token.CollectionID] {
		return task.BalanceAdmissionTargetChanged
	}
	if current.LeaderHash != token.Snapshot.LeaderHash {
		return task.BalanceAdmissionLeaderMissing
	}
	if current.PendingRevision(token.Epoch).EffectiveRevision() != token.Snapshot.PendingRevision(token.Epoch).EffectiveRevision() {
		return task.BalanceAdmissionStaleEpoch
	}
	if !sourcePresent(distribution, token) {
		return task.BalanceAdmissionSourceGone
	}
	return task.BalanceAdmissionAccepted
}

func (b *PlacementSnapshotBuilder) capturePending() task.PendingBalanceSnapshot {
	if b.inspector == nil {
		return task.PendingBalanceSnapshot{}
	}
	return b.inspector.GetPendingBalanceTasks()
}

func (b *PlacementSnapshotBuilder) captureToken(
	ctx context.Context,
	resourceGroup string,
	distribution meta.DistributionSnapshot,
	pending task.PendingBalanceSnapshot,
) (SnapshotToken, bool, error) {
	snapshot, stable, err := b.buildFromCapture(ctx, resourceGroup, nil, distribution, pending, nil)
	if err != nil {
		return SnapshotToken{}, false, err
	}
	if !stable {
		return SnapshotToken{}, false, nil
	}
	return snapshot.Token, true, nil
}

func (b *PlacementSnapshotBuilder) buildFromCapture(
	ctx context.Context,
	resourceGroup string,
	eligibleReplicaIDs []int64,
	distribution meta.DistributionSnapshot,
	pending task.PendingBalanceSnapshot,
	carryOver []task.PendingBalanceTaskSnapshot,
) (*PlacementSnapshot, bool, error) {
	rg := b.meta.ResourceManager.GetResourceGroup(ctx, resourceGroup)
	if rg == nil {
		return nil, false, fmt.Errorf("resource group %q not found", resourceGroup)
	}

	replicas := append([]*meta.Replica(nil), b.meta.ReplicaManager.GetByResourceGroup(ctx, resourceGroup)...)
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].GetID() < replicas[j].GetID() })
	replicaSnapshots := make(map[int64]ReplicaSnapshot, len(replicas))
	collections := make(map[int64]struct{})
	physicalNodes := make(map[int64]struct{})
	scopeNodes := make(map[int64]struct{})
	for _, nodeID := range rg.GetAllNodes() {
		physicalNodes[nodeID] = struct{}{}
		scopeNodes[nodeID] = struct{}{}
	}
	for _, replica := range replicas {
		snapshot := copyReplicaSnapshot(replica)
		replicaSnapshots[snapshot.ID] = snapshot
		collections[snapshot.CollectionID] = struct{}{}
		for _, nodeID := range snapshot.RONodes {
			scopeNodes[nodeID] = struct{}{}
		}
		for _, nodeID := range snapshot.ROSQNodes {
			scopeNodes[nodeID] = struct{}{}
		}
	}

	nodeSnapshots := b.copyNodeSnapshots(rg, scopeNodes, physicalNodes)
	collectionTargets, currentVersions, nextVersions, targetsStable := b.copyTargets(ctx, collections)
	if !targetsStable {
		return nil, false, nil
	}
	segments, channels := projectDistribution(distribution, replicas, physicalNodes)
	eligible := make(map[int64]struct{}, len(eligibleReplicaIDs))
	for _, replicaID := range eligibleReplicaIDs {
		if _, ok := replicaSnapshots[replicaID]; ok {
			eligible[replicaID] = struct{}{}
		}
	}
	mergedPending := mergePendingTasks(resourceGroup, scopeNodes, pending.Tasks, carryOver)
	pendingWork := projectPendingWork(pending.Revision, mergedPending, distribution)
	pendingRevision := pending.RevisionFor(resourceGroup, task.BalanceEpochMeta{})
	pendingEpochRevisions := make(map[task.BalanceEpochMeta]uint64)
	for epoch, revision := range pending.EpochRevisions {
		if epoch.ResourceGroup == resourceGroup {
			pendingEpochRevisions[epoch] = revision
		}
	}

	token := SnapshotToken{
		ResourceGroup:         resourceGroup,
		RGHash:                hashResourceGroup(rg),
		ReplicaHash:           hashReplicas(replicaSnapshots),
		NodeHash:              hashNodes(nodeSnapshots),
		LeaderHash:            hashLeaders(distribution, replicas, physicalNodes),
		PlacementHash:         hashPlacements(segments, channels),
		SegmentRevision:       distribution.SegmentVersion,
		ChannelRevision:       distribution.ChannelVersion,
		PendingTaskRevision:   pendingRevision.Revision,
		PendingGlobalRevision: pending.Revision,
		CurrentTargetVersion:  currentVersions,
		NextTargetVersion:     nextVersions,
		pendingEpochRevisions: pendingEpochRevisions,
	}
	return &PlacementSnapshot{
		Token:             token,
		CapturedAt:        time.Now(),
		Nodes:             nodeSnapshots,
		Replicas:          replicaSnapshots,
		Segments:          segments,
		Channels:          channels,
		CollectionTargets: collectionTargets,
		PendingWork:       pendingWork,
		EligibleReplicas:  eligible,
	}, true, nil
}

func copyReplicaSnapshot(replica *meta.Replica) ReplicaSnapshot {
	copyAndSort := func(values []int64) []int64 {
		copied := append([]int64(nil), values...)
		sort.Slice(copied, func(i, j int) bool { return copied[i] < copied[j] })
		return copied
	}
	mappings := replica.GetChannelRWNodeMappings()
	for channel, nodes := range mappings {
		mappings[channel] = copyAndSort(nodes)
	}
	return ReplicaSnapshot{
		ID:             replica.GetID(),
		CollectionID:   replica.GetCollectionID(),
		ResourceGroup:  replica.GetResourceGroup(),
		RWNodes:        copyAndSort(replica.GetRWNodes()),
		RONodes:        copyAndSort(replica.GetRONodes()),
		RWSQNodes:      copyAndSort(replica.GetRWSQNodes()),
		ROSQNodes:      copyAndSort(replica.GetROSQNodes()),
		ChannelRWNodes: mappings,
	}
}

func (b *PlacementSnapshotBuilder) copyNodeSnapshots(
	rg *meta.ResourceGroup,
	nodeIDs map[int64]struct{},
	physicalNodes map[int64]struct{},
) map[int64]NodeSnapshot {
	snapshots := make(map[int64]NodeSnapshot, len(nodeIDs))
	for nodeID := range nodeIDs {
		snapshot := NodeSnapshot{ID: nodeID}
		if node := b.nodes.Get(nodeID); node != nil {
			snapshot.Exists = true
			snapshot.State = node.GetState()
			snapshot.ResourceGroup = node.ResourceGroupName()
			snapshot.ResourceExhausted = b.nodes.IsResourceExhausted(nodeID)
			snapshot.MemoryCapacity = node.MemCapacity()
			_, physical := physicalNodes[nodeID]
			snapshot.Eligible = physical && snapshot.State == session.NodeStateNormal &&
				!snapshot.ResourceExhausted && rg.AcceptNode(nodeID)
		}
		snapshots[nodeID] = snapshot
	}
	return snapshots
}

func (b *PlacementSnapshotBuilder) copyTargets(
	ctx context.Context,
	collections map[int64]struct{},
) (map[int64]CollectionTargetSnapshot, map[int64]int64, map[int64]int64, bool) {
	targets := make(map[int64]CollectionTargetSnapshot, len(collections))
	currentVersions := make(map[int64]int64, len(collections))
	nextVersions := make(map[int64]int64, len(collections))
	for collectionID := range collections {
		current, stable := b.copyTargetScope(ctx, collectionID, meta.CurrentTarget)
		if !stable {
			return nil, nil, nil, false
		}
		next, stable := b.copyTargetScope(ctx, collectionID, meta.NextTarget)
		if !stable {
			return nil, nil, nil, false
		}
		targets[collectionID] = CollectionTargetSnapshot{Current: current, Next: next}
		currentVersions[collectionID] = current.Version
		nextVersions[collectionID] = next.Version
	}
	return targets, currentVersions, nextVersions, true
}

func (b *PlacementSnapshotBuilder) copyTargetScope(ctx context.Context, collectionID int64, scope meta.TargetScope) (TargetScopeSnapshot, bool) {
	versionBefore := b.target.GetCollectionTargetVersion(ctx, collectionID, scope)
	segments := b.target.GetSealedSegmentsByCollection(ctx, collectionID, scope)
	channels := b.target.GetDmChannelsByCollection(ctx, collectionID, scope)
	snapshot := TargetScopeSnapshot{
		Version:  versionBefore,
		Segments: make(map[int64]TargetSegmentSnapshot, len(segments)),
		Channels: make(map[string]TargetChannelSnapshot, len(channels)),
	}
	for segmentID, segment := range segments {
		if segment == nil {
			continue
		}
		snapshot.Segments[segmentID] = targetSegmentFrom(segment)
	}
	for channelName, channel := range channels {
		primitive := TargetChannelSnapshot{Channel: channelName}
		if channel != nil {
			primitive.CollectionID = channel.GetCollectionID()
			primitive.Channel = channel.GetChannelName()
			primitive.GrowingSegmentIDs = append([]int64(nil), channel.GetUnflushedSegmentIds()...)
			sort.Slice(primitive.GrowingSegmentIDs, func(i, j int) bool { return primitive.GrowingSegmentIDs[i] < primitive.GrowingSegmentIDs[j] })
		}
		snapshot.Channels[channelName] = primitive
	}
	versionAfter := b.target.GetCollectionTargetVersion(ctx, collectionID, scope)
	if versionBefore != versionAfter {
		return TargetScopeSnapshot{}, false
	}
	return snapshot, true
}

func targetSegmentFrom(segment *datapb.SegmentInfo) TargetSegmentSnapshot {
	return TargetSegmentSnapshot{
		ID:           segment.GetID(),
		CollectionID: segment.GetCollectionID(),
		PartitionID:  segment.GetPartitionID(),
		Channel:      segment.GetInsertChannel(),
		RowCount:     segment.GetNumOfRows(),
	}
}

func projectDistribution(
	distribution meta.DistributionSnapshot,
	replicas []*meta.Replica,
	physicalNodes map[int64]struct{},
) (map[SegmentObjectKey][]SegmentPlacement, map[ChannelObjectKey][]ChannelPlacement) {
	segments := make(map[SegmentObjectKey][]SegmentPlacement)
	channels := make(map[ChannelObjectKey][]ChannelPlacement)
	for _, record := range distribution.Segments {
		for _, replicaID := range placementReplicaIDs(replicas, record.CollectionID, record.NodeID, physicalNodes) {
			key := SegmentObjectKey{ReplicaID: replicaID, SegmentID: record.SegmentID, Scope: record.Scope}
			segments[key] = append(segments[key], SegmentPlacement{
				NodeID: record.NodeID, CollectionID: record.CollectionID, PartitionID: record.PartitionID,
				Channel: record.Channel, RowCount: record.RowCount, Version: record.Version, Present: record.Present,
			})
		}
	}
	for _, record := range distribution.Channels {
		for _, replicaID := range placementReplicaIDs(replicas, record.CollectionID, record.NodeID, physicalNodes) {
			channelKey := ChannelObjectKey{ReplicaID: replicaID, Channel: record.Channel}
			channels[channelKey] = append(channels[channelKey], ChannelPlacement{
				NodeID: record.NodeID, CollectionID: record.CollectionID, Version: record.Version, Present: record.Present,
				Serviceable: record.Serviceable, LeaderID: record.LeaderID, LeaderVersion: record.LeaderVersion,
				LeaderTargetVersion: record.LeaderTargetVersion, NumOfGrowingRows: record.NumOfGrowingRows,
			})
			for _, growing := range record.GrowingSegments {
				nodeID := growing.NodeID
				if nodeID == 0 {
					nodeID = record.NodeID
				}
				key := SegmentObjectKey{ReplicaID: replicaID, SegmentID: growing.SegmentID, Scope: querypb.DataScope_Streaming}
				segments[key] = append(segments[key], SegmentPlacement{
					NodeID: nodeID, CollectionID: record.CollectionID, Channel: record.Channel,
					RowCount: growing.RowCount, Version: record.Version, Present: true,
				})
			}
		}
	}
	for key := range segments {
		sort.Slice(segments[key], func(i, j int) bool { return segments[key][i].NodeID < segments[key][j].NodeID })
	}
	for key := range channels {
		sort.Slice(channels[key], func(i, j int) bool { return channels[key][i].NodeID < channels[key][j].NodeID })
	}
	return segments, channels
}

func placementReplicaIDs(replicas []*meta.Replica, collectionID, nodeID int64, physicalNodes map[int64]struct{}) []int64 {
	matched := matchingReplicas(replicas, collectionID, nodeID, physicalNodes)
	ids := make([]int64, 0, len(matched))
	for _, replica := range matched {
		ids = append(ids, replica.GetID())
	}
	if len(ids) == 0 {
		if _, physical := physicalNodes[nodeID]; physical {
			ids = append(ids, meta.NilReplica.GetID())
		}
	}
	return ids
}

func matchingReplicas(replicas []*meta.Replica, collectionID, nodeID int64, physicalNodes map[int64]struct{}) []*meta.Replica {
	matched := make([]*meta.Replica, 0, 1)
	for _, replica := range replicas {
		if replica.GetCollectionID() != collectionID {
			continue
		}
		_, physical := physicalNodes[nodeID]
		if (physical && replica.Contains(nodeID)) || replica.ContainRONode(nodeID) || replica.ContainROSQNode(nodeID) {
			matched = append(matched, replica)
		}
	}
	return matched
}

func mergePendingTasks(
	resourceGroup string,
	scopeNodes map[int64]struct{},
	scheduler, carryOver []task.PendingBalanceTaskSnapshot,
) []task.PendingBalanceTaskSnapshot {
	merged := make(map[string]task.PendingBalanceTaskSnapshot)
	add := func(pending task.PendingBalanceTaskSnapshot) {
		filtered, ok := filterPendingTask(resourceGroup, scopeNodes, pending)
		if !ok {
			return
		}
		pending = filtered
		key := fmt.Sprintf("task/%d", pending.TaskID)
		if pending.TaskID == 0 {
			key = fmt.Sprintf("anonymous/%d/%d/%v", pending.CollectionID, pending.ReplicaID, pending.Actions)
		}
		if _, exists := merged[key]; exists {
			return
		}
		pending.Actions = append([]task.PendingBalanceActionSnapshot(nil), pending.Actions...)
		merged[key] = pending
	}
	for _, pending := range scheduler {
		add(pending)
	}
	for _, pending := range carryOver {
		add(pending)
	}
	tasks := make([]task.PendingBalanceTaskSnapshot, 0, len(merged))
	for _, pending := range merged {
		tasks = append(tasks, pending)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].TaskID != tasks[j].TaskID {
			return tasks[i].TaskID < tasks[j].TaskID
		}
		if tasks[i].CollectionID != tasks[j].CollectionID {
			return tasks[i].CollectionID < tasks[j].CollectionID
		}
		return tasks[i].ReplicaID < tasks[j].ReplicaID
	})
	return tasks
}

func filterPendingTask(
	resourceGroup string,
	scopeNodes map[int64]struct{},
	pending task.PendingBalanceTaskSnapshot,
) (task.PendingBalanceTaskSnapshot, bool) {
	if pending.ResourceGroup == resourceGroup {
		pending.Actions = append([]task.PendingBalanceActionSnapshot(nil), pending.Actions...)
		return pending, true
	}
	if pending.ResourceGroup != "" || pending.ReplicaID != meta.NilReplica.GetID() {
		return task.PendingBalanceTaskSnapshot{}, false
	}

	actions := make([]task.PendingBalanceActionSnapshot, 0, len(pending.Actions))
	for _, action := range pending.Actions {
		if _, ok := scopeNodes[action.NodeID]; ok {
			actions = append(actions, action)
		}
	}
	if len(actions) == 0 {
		return task.PendingBalanceTaskSnapshot{}, false
	}
	pending.Actions = actions
	return pending, true
}

func projectPendingWork(
	revision uint64,
	tasks []task.PendingBalanceTaskSnapshot,
	distribution meta.DistributionSnapshot,
) PendingWorkSnapshot {
	work := PendingWorkSnapshot{
		Revision: revision, Tasks: tasks,
		SegmentWorkloadByNode: make(map[int64]int),
		ChannelWorkloadByNode: make(map[int64]int),
	}
	for _, pending := range tasks {
		for _, action := range pending.Actions {
			if actionReflected(distribution, pending.CollectionID, action) {
				continue
			}
			if action.Channel != "" {
				work.ChannelWorkloadByNode[action.NodeID] += action.Workload
			} else if action.SegmentID != 0 {
				work.SegmentWorkloadByNode[action.NodeID] += action.Workload
			}
		}
	}
	return work
}

func actionReflected(distribution meta.DistributionSnapshot, collectionID int64, action task.PendingBalanceActionSnapshot) bool {
	present := false
	if action.Channel != "" {
		for _, channel := range distribution.Channels {
			if channel.CollectionID == collectionID && channel.Channel == action.Channel && channel.NodeID == action.NodeID && channel.Present {
				present = true
				break
			}
		}
	} else if action.Scope == querypb.DataScope_Streaming {
		for _, channel := range distribution.Channels {
			if channel.CollectionID != collectionID || (action.Shard != "" && channel.Channel != action.Shard) {
				continue
			}
			for _, segment := range channel.GrowingSegments {
				nodeID := segment.NodeID
				if nodeID == 0 {
					nodeID = channel.NodeID
				}
				if segment.SegmentID == action.SegmentID && nodeID == action.NodeID {
					present = true
					break
				}
			}
		}
	} else {
		for _, segment := range distribution.Segments {
			if segment.CollectionID == collectionID && segment.SegmentID == action.SegmentID && segment.NodeID == action.NodeID && segment.Scope == action.Scope && segment.Present {
				present = true
				break
			}
		}
	}
	if action.Type == task.ActionTypeGrow {
		return present
	}
	if action.Type == task.ActionTypeReduce {
		return !present
	}
	return action.Workload == 0
}

func sourcePresent(distribution meta.DistributionSnapshot, token AdmissionToken) bool {
	if token.Segment != nil {
		action := task.PendingBalanceActionSnapshot{
			NodeID: token.ExpectedSourceNode, Type: task.ActionTypeGrow,
			SegmentID: token.Segment.SegmentID, Scope: token.Segment.Scope,
		}
		return actionReflected(distribution, token.CollectionID, action)
	}
	if token.Channel != nil {
		action := task.PendingBalanceActionSnapshot{
			NodeID: token.ExpectedSourceNode, Type: task.ActionTypeGrow, Channel: token.Channel.Channel,
		}
		return actionReflected(distribution, token.CollectionID, action)
	}
	return true
}

type digestWriter struct {
	h hash.Hash64
}

func newDigestWriter() *digestWriter {
	return &digestWriter{h: fnv.New64a()}
}

func (d *digestWriter) writeBytes(value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = d.h.Write(length[:])
	_, _ = d.h.Write(value)
}

func (d *digestWriter) writeString(value string) { d.writeBytes([]byte(value)) }
func (d *digestWriter) writeInt64(value int64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(value))
	_, _ = d.h.Write(encoded[:])
}
func (d *digestWriter) writeUint64(value uint64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	_, _ = d.h.Write(encoded[:])
}
func (d *digestWriter) writeBool(value bool) {
	if value {
		d.writeUint64(1)
	} else {
		d.writeUint64(0)
	}
}
func (d *digestWriter) sum64() uint64 { return d.h.Sum64() }

func hashResourceGroup(rg *meta.ResourceGroup) uint64 {
	digest := newDigestWriter()
	digest.writeString(rg.GetName())
	nodes := append([]int64(nil), rg.GetAllNodes()...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	for _, nodeID := range nodes {
		digest.writeInt64(nodeID)
	}
	config, _ := proto.MarshalOptions{Deterministic: true}.Marshal(rg.GetConfigCloned())
	digest.writeBytes(config)
	return digest.sum64()
}

func hashReplicas(replicas map[int64]ReplicaSnapshot) uint64 {
	digest := newDigestWriter()
	ids := sortedInt64Keys(replicas)
	for _, replicaID := range ids {
		replica := replicas[replicaID]
		digest.writeInt64(replica.ID)
		digest.writeInt64(replica.CollectionID)
		digest.writeString(replica.ResourceGroup)
		for _, values := range [][]int64{replica.RWNodes, replica.RONodes, replica.RWSQNodes, replica.ROSQNodes} {
			digest.writeInt64(int64(len(values)))
			for _, nodeID := range values {
				digest.writeInt64(nodeID)
			}
		}
		channels := make([]string, 0, len(replica.ChannelRWNodes))
		for channel := range replica.ChannelRWNodes {
			channels = append(channels, channel)
		}
		sort.Strings(channels)
		digest.writeInt64(int64(len(channels)))
		for _, channel := range channels {
			digest.writeString(channel)
			digest.writeInt64(int64(len(replica.ChannelRWNodes[channel])))
			for _, nodeID := range replica.ChannelRWNodes[channel] {
				digest.writeInt64(nodeID)
			}
		}
	}
	return digest.sum64()
}

func hashPlacements(
	segments map[SegmentObjectKey][]SegmentPlacement,
	channels map[ChannelObjectKey][]ChannelPlacement,
) uint64 {
	digest := newDigestWriter()
	segmentKeys := make([]SegmentObjectKey, 0, len(segments))
	for key := range segments {
		segmentKeys = append(segmentKeys, key)
	}
	sort.Slice(segmentKeys, func(i, j int) bool {
		left, right := segmentKeys[i], segmentKeys[j]
		if left.ReplicaID != right.ReplicaID {
			return left.ReplicaID < right.ReplicaID
		}
		if left.SegmentID != right.SegmentID {
			return left.SegmentID < right.SegmentID
		}
		return left.Scope < right.Scope
	})
	digest.writeInt64(int64(len(segmentKeys)))
	for _, key := range segmentKeys {
		digest.writeInt64(key.ReplicaID)
		digest.writeInt64(key.SegmentID)
		digest.writeInt64(int64(key.Scope))
		placements := segments[key]
		digest.writeInt64(int64(len(placements)))
		for _, placement := range placements {
			digest.writeInt64(placement.NodeID)
			digest.writeInt64(placement.CollectionID)
			digest.writeInt64(placement.PartitionID)
			digest.writeString(placement.Channel)
			digest.writeInt64(placement.RowCount)
			digest.writeInt64(placement.Version)
			digest.writeBool(placement.Present)
		}
	}

	channelKeys := make([]ChannelObjectKey, 0, len(channels))
	for key := range channels {
		channelKeys = append(channelKeys, key)
	}
	sort.Slice(channelKeys, func(i, j int) bool {
		left, right := channelKeys[i], channelKeys[j]
		if left.ReplicaID != right.ReplicaID {
			return left.ReplicaID < right.ReplicaID
		}
		return left.Channel < right.Channel
	})
	digest.writeInt64(int64(len(channelKeys)))
	for _, key := range channelKeys {
		digest.writeInt64(key.ReplicaID)
		digest.writeString(key.Channel)
		placements := channels[key]
		digest.writeInt64(int64(len(placements)))
		for _, placement := range placements {
			digest.writeInt64(placement.NodeID)
			digest.writeInt64(placement.CollectionID)
			digest.writeInt64(placement.Version)
			digest.writeBool(placement.Present)
			digest.writeBool(placement.Serviceable)
			digest.writeInt64(placement.LeaderID)
			digest.writeInt64(placement.LeaderVersion)
			digest.writeInt64(placement.LeaderTargetVersion)
			digest.writeInt64(placement.NumOfGrowingRows)
		}
	}
	return digest.sum64()
}

func hashNodes(nodes map[int64]NodeSnapshot) uint64 {
	digest := newDigestWriter()
	ids := sortedInt64Keys(nodes)
	for _, nodeID := range ids {
		node := nodes[nodeID]
		digest.writeInt64(node.ID)
		digest.writeBool(node.Exists)
		digest.writeBool(node.Eligible)
		digest.writeInt64(int64(node.State))
		digest.writeString(node.ResourceGroup)
		digest.writeBool(node.ResourceExhausted)
		digest.writeUint64(math.Float64bits(node.MemoryCapacity))
	}
	return digest.sum64()
}

type leaderDigestRecord struct {
	replicaID    int64
	collectionID int64
	channel      string
	nodeID       int64
	leaderID     int64
	version      int64
	target       int64
	serviceable  bool
}

func hashLeaders(distribution meta.DistributionSnapshot, replicas []*meta.Replica, physicalNodes map[int64]struct{}) uint64 {
	records := make([]leaderDigestRecord, 0)
	for _, channel := range distribution.Channels {
		for _, replica := range matchingReplicas(replicas, channel.CollectionID, channel.NodeID, physicalNodes) {
			records = append(records, leaderDigestRecord{
				replicaID: replica.GetID(), collectionID: channel.CollectionID, channel: channel.Channel,
				nodeID: channel.NodeID, leaderID: channel.LeaderID, version: channel.LeaderVersion,
				target: channel.LeaderTargetVersion, serviceable: channel.Serviceable,
			})
		}
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.replicaID != right.replicaID {
			return left.replicaID < right.replicaID
		}
		if left.channel != right.channel {
			return left.channel < right.channel
		}
		return left.nodeID < right.nodeID
	})
	digest := newDigestWriter()
	for _, record := range records {
		digest.writeInt64(record.replicaID)
		digest.writeInt64(record.collectionID)
		digest.writeString(record.channel)
		digest.writeInt64(record.nodeID)
		digest.writeInt64(record.leaderID)
		digest.writeInt64(record.version)
		digest.writeInt64(record.target)
		digest.writeBool(record.serviceable)
	}
	return digest.sum64()
}

func sortedInt64Keys[V any](values map[int64]V) []int64 {
	keys := make([]int64, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
