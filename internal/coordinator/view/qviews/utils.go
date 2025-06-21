package qviews

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// Preparing -> Ready: If all worknodes are ready.
// Preparing -> Unrecoverable: If any worknode are unrecoverable, or the query view is deprecated when preparing by balancer.
// Ready -> Up: If the streamingnode is up.
// Up -> Down: By Down interface.
// Down -> Dropping: If the streamingnode is down.
// Unrecoverable -> Dropping: By DropView interface.
// Dropping -> Dropped: If all worknodes are dropped.
const (
	QueryViewStatePreparing     = QueryViewState(viewpb.QueryViewState_QueryViewStatePreparing)
	QueryViewStateReady         = QueryViewState(viewpb.QueryViewState_QueryViewStateReady)
	QueryViewStateUp            = QueryViewState(viewpb.QueryViewState_QueryViewStateUp)
	QueryViewStateDown          = QueryViewState(viewpb.QueryViewState_QueryViewStateDown)
	QueryViewStateUnrecoverable = QueryViewState(viewpb.QueryViewState_QueryViewStateUnrecoverable)
	QueryViewStateDropping      = QueryViewState(viewpb.QueryViewState_QueryViewStateDropping)
	QueryViewStateDropped       = QueryViewState(viewpb.QueryViewState_QueryViewStateDropped)
	QueryViewStateNil           = QueryViewState(viewpb.QueryViewState_QueryViewStateUnknown)
)

type (
	QueryViewState viewpb.QueryViewState

	NodeSyncState int // NodeSyncState marks the on-syncing node state.
)

func (s QueryViewState) String() string {
	return viewpb.QueryViewState(s).String()
}

// ShardID is the unique identifier of a shard.
type ShardID struct {
	ReplicaID int64
	VChannel  string
}

// String returns the string representation of the shard id.
func (id ShardID) String() string {
	return fmt.Sprintf("%d-%s", id.ReplicaID, id.VChannel)
}

// NewShardIDFromQVMeta creates a new shard id from the query view meta.
func NewShardIDFromQVMeta(meta *viewpb.QueryViewMeta) ShardID {
	return ShardID{
		ReplicaID: meta.ReplicaId,
		VChannel:  meta.Vchannel,
	}
}

// NewStateTransition creates a new state transition.
func NewStateTransition(from QueryViewState) StateTransition {
	return StateTransition{
		From: from,
		To:   QueryViewStateNil,
	}
}

// StateTransition is the transition of the query view state.
type StateTransition struct {
	From QueryViewState
	To   QueryViewState
}

// Done returns true if the transition is done.
func (s *StateTransition) Done(to QueryViewState) {
	s.To = to
}

// IsStateTransition returns true if the transition is a state transition.
func (s StateTransition) IsStateTransition() bool {
	if s.To == QueryViewStateNil {
		panic("please call Done before IsStateTransition")
	}
	return s.From != s.To
}

// FromProtoQueryViewVersion converts a QueryViewVersion proto to a QueryViewVersion.
func FromProtoQueryViewVersion(qvv *viewpb.QueryViewVersion) QueryViewVersion {
	return QueryViewVersion{
		DataVersion:  qvv.DataVersion,
		QueryVersion: qvv.QueryVersion,
	}
}

// QueryViewVersion is the version of the query view.
type QueryViewVersion struct {
	DataVersion  int64
	QueryVersion int64
}

// String returns the string representation of the query view version.
func (qv QueryViewVersion) String() string {
	return fmt.Sprintf("%d/%d", qv.DataVersion, qv.QueryVersion)
}

// GTE returns true if qv is greater than or equal to qv2.
func (qv QueryViewVersion) GTE(qv2 QueryViewVersion) bool {
	return qv.DataVersion > qv2.DataVersion ||
		(qv.DataVersion == qv2.DataVersion && qv.QueryVersion >= qv2.QueryVersion)
}

// GT returns true if qv is greater than qv2.
func (qv QueryViewVersion) GT(qv2 QueryViewVersion) bool {
	return qv.DataVersion > qv2.DataVersion ||
		(qv.DataVersion == qv2.DataVersion && qv.QueryVersion > qv2.QueryVersion)
}

// EQ returns true if qv is equal to qv2.
func (qv QueryViewVersion) EQ(qv2 QueryViewVersion) bool {
	return qv.DataVersion == qv2.DataVersion && qv.QueryVersion == qv2.QueryVersion
}
