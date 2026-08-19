package channel

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

const (
	defaultWALReplicaID        int64 = 0
	firstAllocatedWALReplicaID int64 = 1
)

// NewPChannelMeta creates a new PChannelMeta.
// By default, the channel is available in replication.
func NewPChannelMeta(name string, accessMode types.AccessMode) *PChannelMeta {
	return newPChannelMetaWithAvailability(name, accessMode, true)
}

// newPChannelMetaWithAvailability creates a new PChannelMeta with explicit availability in replication.
func newPChannelMetaWithAvailability(name string, accessMode types.AccessMode, availableInReplication bool) *PChannelMeta {
	return &PChannelMeta{
		inner: normalizePChannelMeta(&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{
				Name:       name,
				Term:       1,
				AccessMode: streamingpb.PChannelAccessMode(accessMode),
			},
			Node:             nil,
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
			Histories:        make([]*streamingpb.PChannelAssignmentLog, 0),
			PrimaryReplicaId: defaultWALReplicaID,
			NextReplicaId:    firstAllocatedWALReplicaID,
		}),
		availableInReplication: availableInReplication,
	}
}

// newPChannelMetaFromProto creates a new PChannelMeta from proto.
// The availableInReplication flag is computed from the given replicateConfig.
func newPChannelMetaFromProto(channel *streamingpb.PChannelMeta, replicateConfig *replicateutil.ConfigHelper) *PChannelMeta {
	channel = normalizePChannelMeta(channel)
	return &PChannelMeta{
		inner:                  channel,
		availableInReplication: isChannelAvailableInReplication(channel.GetChannel().GetName(), replicateConfig),
	}
}

func normalizePChannelMeta(meta *streamingpb.PChannelMeta) *streamingpb.PChannelMeta {
	if meta == nil {
		meta = &streamingpb.PChannelMeta{}
	}
	if len(meta.Replicas) == 0 {
		meta.PrimaryReplicaId = defaultWALReplicaID
		if meta.NextReplicaId <= defaultWALReplicaID {
			meta.NextReplicaId = firstAllocatedWALReplicaID
		}
		meta.Replicas = []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:                  defaultWALReplicaID,
				AccessMode:                 meta.GetChannel().GetAccessMode(),
				AssignmentEpoch:            0,
				ActiveNode:                 cloneStreamingNodeInfo(meta.GetNode()),
				State:                      meta.GetState(),
				Histories:                  cloneAssignmentLogs(meta.GetHistories()),
				LastAssignTimestampSeconds: meta.GetLastAssignTimestampSeconds(),
			},
		}
		return meta
	}
	if meta.NextReplicaId <= meta.PrimaryReplicaId {
		meta.NextReplicaId = meta.PrimaryReplicaId + 1
	}
	if meta.NextReplicaId <= defaultWALReplicaID {
		meta.NextReplicaId = firstAllocatedWALReplicaID
	}
	normalizeWALReplicaAccessModes(meta)
	syncPrimaryReplicaToLegacyProjection(meta)
	return meta
}

func cloneStreamingNodeInfo(node *streamingpb.StreamingNodeInfo) *streamingpb.StreamingNodeInfo {
	if node == nil {
		return nil
	}
	return proto.Clone(node).(*streamingpb.StreamingNodeInfo)
}

func cloneAssignmentLogs(histories []*streamingpb.PChannelAssignmentLog) []*streamingpb.PChannelAssignmentLog {
	if len(histories) == 0 {
		return make([]*streamingpb.PChannelAssignmentLog, 0)
	}
	cloned := make([]*streamingpb.PChannelAssignmentLog, 0, len(histories))
	for _, history := range histories {
		cloned = append(cloned, proto.Clone(history).(*streamingpb.PChannelAssignmentLog))
	}
	return cloned
}

func primaryReplica(meta *streamingpb.PChannelMeta) *streamingpb.WALReplicaAssignment {
	for _, replica := range meta.GetReplicas() {
		if replica.GetReplicaId() == meta.GetPrimaryReplicaId() {
			return replica
		}
	}
	if len(meta.GetReplicas()) == 0 {
		return nil
	}
	meta.PrimaryReplicaId = meta.GetReplicas()[0].GetReplicaId()
	return meta.GetReplicas()[0]
}

func findReplica(meta *streamingpb.PChannelMeta, replicaID int64) *streamingpb.WALReplicaAssignment {
	for _, replica := range meta.GetReplicas() {
		if replica.GetReplicaId() == replicaID {
			return replica
		}
	}
	return nil
}

func normalizeWALReplicaAccessModes(meta *streamingpb.PChannelMeta) {
	var primary *streamingpb.WALReplicaAssignment
	var firstReadWrite *streamingpb.WALReplicaAssignment
	for _, replica := range meta.GetReplicas() {
		if replica.GetReplicaId() == meta.GetPrimaryReplicaId() {
			primary = replica
		}
		if firstReadWrite == nil && replica.GetAccessMode() == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
			firstReadWrite = replica
		}
	}
	if firstReadWrite == nil {
		return
	}
	if primary == nil || primary.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
		primary = firstReadWrite
		meta.PrimaryReplicaId = primary.GetReplicaId()
	}
	for _, replica := range meta.GetReplicas() {
		if replica.GetReplicaId() == primary.GetReplicaId() {
			continue
		}
		if replica.GetAccessMode() == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
			replica.AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY
			replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
		}
	}
}

func syncPrimaryReplicaToLegacyProjection(meta *streamingpb.PChannelMeta) {
	replica := primaryReplica(meta)
	if replica == nil {
		return
	}
	if meta.Channel != nil {
		meta.Channel.AccessMode = replica.GetAccessMode()
	}
	meta.Node = cloneStreamingNodeInfo(replica.GetActiveNode())
	if replica.GetState() == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING && replica.GetTargetNode() != nil {
		meta.Node = cloneStreamingNodeInfo(replica.GetTargetNode())
	}
	meta.State = replica.GetState()
	meta.Histories = cloneAssignmentLogs(replica.GetHistories())
	meta.LastAssignTimestampSeconds = replica.GetLastAssignTimestampSeconds()
}

// PChannelMeta is the read only version of PChannelInfo, to be used in balancer,
// If you need to update PChannelMeta, please use CopyForWrite to get mutablePChannel.
type PChannelMeta struct {
	inner                  *streamingpb.PChannelMeta
	availableInReplication bool
}

// AvailableInReplication returns whether the channel is available for VChannel allocation
// and DDL broadcasts. Dynamically-added PChannels are gated until they appear in ReplicateConfig.
func (c *PChannelMeta) AvailableInReplication() bool {
	return c.availableInReplication
}

// Name returns the name of the channel.
func (c *PChannelMeta) Name() string {
	return c.inner.GetChannel().GetName()
}

// ChannelID returns the channel id.
func (c *PChannelMeta) ChannelID() types.ChannelID {
	return types.ChannelID{Name: c.inner.Channel.Name}
}

// ChannelInfo returns the channel info.
func (c *PChannelMeta) ChannelInfo() types.PChannelInfo {
	return types.NewPChannelInfoFromProto(c.inner.Channel)
}

// PrimaryReplicaID returns the WAL replica id of the primary projection.
func (c *PChannelMeta) PrimaryReplicaID() int64 {
	return c.inner.GetPrimaryReplicaId()
}

// NextReplicaID returns the next WAL replica id that should be allocated.
func (c *PChannelMeta) NextReplicaID() int64 {
	return c.inner.GetNextReplicaId()
}

// Replicas returns the WAL replica assignments of this PChannel.
func (c *PChannelMeta) Replicas() []*streamingpb.WALReplicaAssignment {
	replicas := make([]*streamingpb.WALReplicaAssignment, 0, len(c.inner.GetReplicas()))
	for _, replica := range c.inner.GetReplicas() {
		replicas = append(replicas, proto.Clone(replica).(*streamingpb.WALReplicaAssignment))
	}
	return replicas
}

// WALReplica returns a cloned WAL replica assignment.
func (c *PChannelMeta) WALReplica(replicaID int64) (*streamingpb.WALReplicaAssignment, bool) {
	replica := findReplica(c.inner, replicaID)
	if replica == nil {
		return nil, false
	}
	return proto.Clone(replica).(*streamingpb.WALReplicaAssignment), true
}

// Term returns the current term of the channel.
func (c *PChannelMeta) CurrentTerm() int64 {
	return c.inner.GetChannel().GetTerm()
}

// CurrentServerID returns the server id of the channel.
// If the channel is not assigned to any server, return -1.
func (c *PChannelMeta) CurrentServerID() int64 {
	return c.inner.GetNode().GetServerId()
}

// CurrentAssignment returns the current assignment of the channel.
func (c *PChannelMeta) CurrentAssignment() types.PChannelInfoAssigned {
	replica := primaryReplica(c.inner)
	var assignmentEpoch int64
	if replica != nil {
		assignmentEpoch = replica.GetAssignmentEpoch()
	}
	return types.PChannelInfoAssigned{
		Channel:         types.NewPChannelInfoFromProto(c.inner.Channel),
		WALReplicaID:    c.PrimaryReplicaID(),
		AssignmentEpoch: assignmentEpoch,
		Node:            types.NewStreamingNodeInfoFromProto(c.inner.Node),
	}
}

// AssignHistories returns the history of the channel assignment.
func (c *PChannelMeta) AssignHistories() []types.PChannelInfoAssigned {
	history := make([]types.PChannelInfoAssigned, 0, len(c.inner.Histories))
	for _, h := range c.inner.Histories {
		history = append(history, types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{
				Name:       c.inner.GetChannel().GetName(),
				Term:       h.Term,
				AccessMode: types.AccessMode(h.AccessMode),
			},
			WALReplicaID:    c.PrimaryReplicaID(),
			AssignmentEpoch: h.GetAssignmentEpoch(),
			Node:            types.NewStreamingNodeInfoFromProto(h.Node),
		})
	}
	return history
}

// IsAssigned returns if the channel is assigned to a server.
func (c *PChannelMeta) IsAssigned() bool {
	return c.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
}

// IsAssignedOrAssigning returns if the channel is assigned or assigning to a server.
func (c *PChannelMeta) IsAssignedOrAssigning() bool {
	return c.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED || c.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
}

// LastAssignTimestamp returns the last assigned timestamp.
func (c *PChannelMeta) LastAssignTimestamp() time.Time {
	return time.Unix(int64(c.inner.LastAssignTimestampSeconds), 0)
}

// State returns the state of the channel.
func (c *PChannelMeta) State() streamingpb.PChannelMetaState {
	return c.inner.State
}

// CopyForWrite returns mutablePChannel to modify pchannel
// but didn't affect other replicas.
func (c *PChannelMeta) CopyForWrite() *mutablePChannel {
	return &mutablePChannel{
		PChannelMeta: &PChannelMeta{
			inner:                  proto.Clone(c.inner).(*streamingpb.PChannelMeta),
			availableInReplication: c.availableInReplication,
		},
	}
}

// mutablePChannel is a mutable version of PChannel.
// use to update the channel info.
type mutablePChannel struct {
	*PChannelMeta
}

// CreateReadOnlyWALReplica appends a read-only WAL replica entry and returns its stable replica id.
func (m *mutablePChannel) CreateReadOnlyWALReplica(resourceGroup string) int64 {
	replicaID := m.inner.GetNextReplicaId()
	if replicaID <= defaultWALReplicaID {
		replicaID = firstAllocatedWALReplicaID
	}
	m.inner.NextReplicaId = replicaID + 1
	m.inner.Replicas = append(m.inner.Replicas, &streamingpb.WALReplicaAssignment{
		ReplicaId:       replicaID,
		AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
		ResourceGroup:   resourceGroup,
		State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
		Histories:       make([]*streamingpb.PChannelAssignmentLog, 0),
		AssignmentEpoch: 0,
	})
	return replicaID
}

// TryAssignWALReplicaToServerID prepares a WAL replica on the given StreamingNode.
func (m *mutablePChannel) TryAssignWALReplicaToServerID(replicaID int64, streamingNode types.StreamingNodeInfo) bool {
	replica := findReplica(m.inner, replicaID)
	if replica == nil {
		return false
	}
	previousState := replica.GetState()
	if replica.GetState() == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED &&
		replica.GetActiveNode().GetServerId() == streamingNode.ServerID {
		return false
	}
	if replica.GetState() == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING &&
		replica.GetTargetNode().GetServerId() == streamingNode.ServerID {
		return false
	}
	if replica.GetState() == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING {
		return false
	}
	updateOrAppendWALReplicaAssignHistory(m.inner.GetChannel().GetTerm(), replica)
	replica.AssignmentEpoch++
	if previousState == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE {
		replica.ActiveNode = nil
	}
	replica.TargetNode = types.NewProtoFromStreamingNodeInfo(streamingNode)
	replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
	if replica.GetAccessMode() == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
		m.inner.Channel.Term++
	}
	syncPrimaryReplicaToLegacyProjection(m.inner)
	return true
}

func updateOrAppendWALReplicaAssignHistory(term int64, replica *streamingpb.WALReplicaAssignment) {
	node := replica.GetActiveNode()
	if node == nil {
		node = replica.GetTargetNode()
	}
	if node == nil {
		return
	}
	updateOrAppendWALReplicaAssignHistoryForNode(term, replica, node, replica.GetAssignmentEpoch())
}

func updateOrAppendWALReplicaAssignHistoryForNode(
	term int64,
	replica *streamingpb.WALReplicaAssignment,
	node *streamingpb.StreamingNodeInfo,
	assignmentEpoch int64,
) {
	if node == nil {
		return
	}
	for _, h := range replica.Histories {
		if h.GetNode().GetServerId() == node.GetServerId() && h.GetAccessMode() == replica.GetAccessMode() {
			h.Term = term
			h.AssignmentEpoch = assignmentEpoch
			return
		}
	}
	replica.Histories = append(replica.Histories, &streamingpb.PChannelAssignmentLog{
		Term:            term,
		Node:            cloneStreamingNodeInfo(node),
		AccessMode:      replica.GetAccessMode(),
		AssignmentEpoch: assignmentEpoch,
	})
}

// AssignWALReplicaToServerDone makes the prepared target of the WAL replica serviceable.
func (m *mutablePChannel) AssignWALReplicaToServerDone(replicaID int64, assignmentEpoch int64) bool {
	replica := findReplica(m.inner, replicaID)
	if replica == nil ||
		replica.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING ||
		replica.GetAssignmentEpoch() != assignmentEpoch {
		return false
	}
	if replica.GetTargetNode() != nil {
		replica.ActiveNode = cloneStreamingNodeInfo(replica.GetTargetNode())
	}
	replica.TargetNode = nil
	replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
	replica.LastAssignTimestampSeconds = uint64(time.Now().Unix())
	syncPrimaryReplicaToLegacyProjection(m.inner)
	return true
}

// ClearWALReplicaHistories clears completed cleanup histories for the WAL replica.
func (m *mutablePChannel) ClearWALReplicaHistories(replicaID int64) bool {
	replica := findReplica(m.inner, replicaID)
	if replica == nil || len(replica.GetHistories()) == 0 {
		return false
	}
	replica.Histories = make([]*streamingpb.PChannelAssignmentLog, 0)
	syncPrimaryReplicaToLegacyProjection(m.inner)
	return true
}

// SwitchPrimaryWALReplica promotes a serviceable read-only WAL replica to be the primary writer.
func (m *mutablePChannel) SwitchPrimaryWALReplica(targetReplicaID int64) bool {
	if targetReplicaID == m.inner.GetPrimaryReplicaId() {
		return false
	}
	oldPrimary := primaryReplica(m.inner)
	target := findReplica(m.inner, targetReplicaID)
	if oldPrimary == nil || target == nil {
		return false
	}
	if oldPrimary.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
		return false
	}
	if oldPrimary.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED ||
		oldPrimary.GetActiveNode() == nil ||
		oldPrimary.GetTargetNode() != nil {
		return false
	}
	if target.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY ||
		target.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED ||
		target.GetActiveNode() == nil ||
		target.GetTargetNode() != nil {
		return false
	}

	m.inner.Channel.Term++
	m.inner.PrimaryReplicaId = targetReplicaID

	oldPrimary.AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY
	oldPrimary.AssignmentEpoch++
	oldPrimary.TargetNode = nil
	oldPrimary.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED

	target.AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
	target.AssignmentEpoch++
	target.TargetNode = nil
	target.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
	syncPrimaryReplicaToLegacyProjection(m.inner)
	return true
}

// MarkWALReplicaAsDropping marks a non-primary read-only WAL replica as dropping.
func (m *mutablePChannel) MarkWALReplicaAsDropping(replicaID int64) bool {
	if replicaID == m.inner.GetPrimaryReplicaId() {
		return false
	}
	replica := findReplica(m.inner, replicaID)
	if replica == nil ||
		replica.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY {
		return false
	}
	if replica.GetState() == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING {
		return true
	}
	switch replica.GetState() {
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE,
		streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING:
		term := m.inner.GetChannel().GetTerm()
		assignmentEpoch := replica.GetAssignmentEpoch()
		updateOrAppendWALReplicaAssignHistoryForNode(term, replica, replica.GetActiveNode(), walReplicaCleanupEpochForNode(replica, replica.GetActiveNode()))
		updateOrAppendWALReplicaAssignHistoryForNode(term, replica, replica.GetTargetNode(), assignmentEpoch)
		replica.AssignmentEpoch++
		replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING
		return true
	default:
		return false
	}
}

func walReplicaCleanupEpochForNode(replica *streamingpb.WALReplicaAssignment, node *streamingpb.StreamingNodeInfo) int64 {
	if node == nil {
		return replica.GetAssignmentEpoch()
	}
	for _, history := range replica.GetHistories() {
		if history.GetNode().GetServerId() == node.GetServerId() &&
			history.GetAccessMode() == replica.GetAccessMode() {
			return history.GetAssignmentEpoch()
		}
	}
	return replica.GetAssignmentEpoch()
}

// MarkWALReplicaAsUnavailable marks a non-primary read-only WAL replica as unavailable.
func (m *mutablePChannel) MarkWALReplicaAsUnavailable(replicaID int64, assignmentEpoch int64) bool {
	if replicaID == m.inner.GetPrimaryReplicaId() {
		return false
	}
	replica := findReplica(m.inner, replicaID)
	if replica == nil ||
		replica.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY ||
		replica.GetAssignmentEpoch() != assignmentEpoch {
		return false
	}
	switch replica.GetState() {
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED:
		replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
		return true
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING:
		if replica.GetActiveNode() == nil {
			return false
		}
		replica.ActiveNode = nil
		return true
	default:
		return false
	}
}

// MarkPrimaryWALReplicaAsUnavailable marks a failed primary write-chain open as unavailable.
func (m *mutablePChannel) MarkPrimaryWALReplicaAsUnavailable(replicaID int64, assignmentEpoch int64) bool {
	if replicaID != m.inner.GetPrimaryReplicaId() {
		return false
	}
	replica := findReplica(m.inner, replicaID)
	if replica == nil ||
		replica.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE ||
		replica.GetAssignmentEpoch() != assignmentEpoch {
		return false
	}
	switch replica.GetState() {
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING:
		replica.TargetNode = nil
		replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
		syncPrimaryReplicaToLegacyProjection(m.inner)
		return true
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED:
		replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
		syncPrimaryReplicaToLegacyProjection(m.inner)
		return true
	default:
		return false
	}
}

// RemoveWALReplica removes a non-primary WAL replica entry from the PChannel meta.
func (m *mutablePChannel) RemoveWALReplica(replicaID int64) bool {
	if replicaID == m.inner.GetPrimaryReplicaId() {
		return false
	}
	for idx, replica := range m.inner.GetReplicas() {
		if replica.GetReplicaId() != replicaID {
			continue
		}
		if replica.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING {
			return false
		}
		m.inner.Replicas = append(m.inner.Replicas[:idx], m.inner.Replicas[idx+1:]...)
		return true
	}
	return false
}

// TryAssignToServerID assigns the channel to a server.
func (m *mutablePChannel) TryAssignToServerID(accessMode types.AccessMode, streamingNode types.StreamingNodeInfo) bool {
	if m.ChannelInfo().AccessMode == accessMode &&
		m.CurrentServerID() == streamingNode.ServerID &&
		(m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED ||
			m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING) {
		// if the channel is already assigned to the server, return false.
		return false
	}
	if m.inner.State != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED {
		m.updateOrAppendAssignHistory()
	}

	// otherwise update the channel into assgining state.
	m.inner.Channel.AccessMode = streamingpb.PChannelAccessMode(accessMode)
	m.inner.Channel.Term++
	m.inner.Node = types.NewProtoFromStreamingNodeInfo(streamingNode)
	m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
	replica := primaryReplica(m.inner)
	replica.AccessMode = streamingpb.PChannelAccessMode(accessMode)
	replica.AssignmentEpoch++
	replica.TargetNode = types.NewProtoFromStreamingNodeInfo(streamingNode)
	replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
	replica.Histories = cloneAssignmentLogs(m.inner.GetHistories())
	replica.LastAssignTimestampSeconds = m.inner.GetLastAssignTimestampSeconds()
	syncPrimaryReplicaToLegacyProjection(m.inner)
	return true
}

// updateOrAppendAssignHistory updates the assign history of the channel if channel is assigned at previous term at target node,
// otherwise, append the history directly.
func (m *mutablePChannel) updateOrAppendAssignHistory() {
	// if the node has been assigned to, update the history directly.
	// e.g. the node 10 is assigned to the channel at term 1 but open failed,
	// we have history record like:
	// (term 1, node 10, access mode RW)
	// (term 2, node 11, access mode RW)
	// the the node is reassigned to the channel at term 3.
	// the the history can be compacted into
	// (term 3, node 10, access mode RW)
	// (term 2, node 11, access mode RW)
	// to make the history smaller.
	for _, h := range m.inner.Histories {
		if h.Node.ServerId == m.inner.Node.ServerId && h.AccessMode == m.inner.Channel.AccessMode {
			h.Term = m.inner.Channel.Term
			h.AssignmentEpoch = primaryReplica(m.inner).GetAssignmentEpoch()
			return
		}
	}
	// otherwise, append the history directly.
	m.inner.Histories = append(m.inner.Histories, &streamingpb.PChannelAssignmentLog{
		Term:            m.inner.Channel.Term,
		Node:            m.inner.Node,
		AccessMode:      m.inner.Channel.AccessMode,
		AssignmentEpoch: primaryReplica(m.inner).GetAssignmentEpoch(),
	})
}

// AssignToServerDone assigns the channel to the server done.
func (m *mutablePChannel) AssignToServerDone() {
	if m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING {
		m.inner.Histories = make([]*streamingpb.PChannelAssignmentLog, 0)
		m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
		m.inner.LastAssignTimestampSeconds = uint64(time.Now().Unix())
		replica := primaryReplica(m.inner)
		replica.ActiveNode = cloneStreamingNodeInfo(replica.GetTargetNode())
		replica.TargetNode = nil
		replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
		replica.Histories = make([]*streamingpb.PChannelAssignmentLog, 0)
		replica.LastAssignTimestampSeconds = m.inner.GetLastAssignTimestampSeconds()
		syncPrimaryReplicaToLegacyProjection(m.inner)
	}
}

// MarkAsUnavailable marks the channel as unavailable.
func (m *mutablePChannel) MarkAsUnavailable(term int64) {
	if m.inner.GetPrimaryReplicaId() != defaultWALReplicaID {
		return
	}
	if m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED && m.CurrentTerm() == term {
		m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
		replica := primaryReplica(m.inner)
		replica.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
		syncPrimaryReplicaToLegacyProjection(m.inner)
	}
}

// IntoRawMeta returns the raw meta, no longger available after call.
func (m *mutablePChannel) IntoRawMeta() *streamingpb.PChannelMeta {
	c := m.PChannelMeta
	m.PChannelMeta = nil
	syncPrimaryReplicaToLegacyProjection(c.inner)
	return c.inner
}
