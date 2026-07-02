package qviews

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryViewState constants mapped from proto.
const (
	QueryViewStatePreparing     = QueryViewState(viewpb.QueryViewState_QueryViewStatePreparing)
	QueryViewStateReady         = QueryViewState(viewpb.QueryViewState_QueryViewStateReady)
	QueryViewStateUp            = QueryViewState(viewpb.QueryViewState_QueryViewStateUp)
	QueryViewStateDown          = QueryViewState(viewpb.QueryViewState_QueryViewStateDown)
	QueryViewStateUnrecoverable = QueryViewState(viewpb.QueryViewState_QueryViewStateUnrecoverable)
	QueryViewStateDropping      = QueryViewState(viewpb.QueryViewState_QueryViewStateDropping)
	QueryViewStateDropped       = QueryViewState(viewpb.QueryViewState_QueryViewStateDropped)
	// StreamingNode-only: WAL is recovering after SN crash.
	// Not used by Coord or QueryNode.
	QueryViewStateUpRecovering = QueryViewState(viewpb.QueryViewState_QueryViewStateUpRecovering)
	QueryViewStateNil          = QueryViewState(viewpb.QueryViewState_QueryViewStateUnknown)
)

// QueryViewState is the state of a query view.
type QueryViewState viewpb.QueryViewState

// String returns the string representation of the query view state.
func (s QueryViewState) String() string {
	return viewpb.QueryViewState(s).String()
}

// ShardID is the unique identifier of a shard (replica + vchannel).
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

// NewStateTransition creates a new state transition from the given state.
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

// Done marks the transition target state.
func (s *StateTransition) Done(to QueryViewState) {
	s.To = to
}

// IsStateTransition returns true if the state actually changed.
func (s StateTransition) IsStateTransition() bool {
	if s.To == QueryViewStateNil {
		panic("please call Done before IsStateTransition")
	}
	return s.From != s.To
}

// DataVersion is the composite version of a data view.
// Ordered lexicographically by (StreamingVersion, CompactVersion).
type DataVersion struct {
	StreamingVersion int64
	CompactVersion   int64
}

// String returns the string representation of the data version.
func (dv DataVersion) String() string {
	return fmt.Sprintf("(%d,%d)", dv.StreamingVersion, dv.CompactVersion)
}

// EQ returns true if dv is equal to other.
func (dv DataVersion) EQ(other DataVersion) bool {
	return dv.StreamingVersion == other.StreamingVersion && dv.CompactVersion == other.CompactVersion
}

// GT returns true if dv is strictly greater than other (lexicographic).
func (dv DataVersion) GT(other DataVersion) bool {
	if dv.StreamingVersion != other.StreamingVersion {
		return dv.StreamingVersion > other.StreamingVersion
	}
	return dv.CompactVersion > other.CompactVersion
}

// GTE returns true if dv is greater than or equal to other.
func (dv DataVersion) GTE(other DataVersion) bool {
	return dv.EQ(other) || dv.GT(other)
}

// FromProtoDataVersion converts a DataVersion proto to a DataVersion.
func FromProtoDataVersion(dv *viewpb.DataVersion) DataVersion {
	return DataVersion{
		StreamingVersion: dv.StreamingVersion,
		CompactVersion:   dv.CompactVersion,
	}
}

// IntoProto converts a DataVersion to a proto DataVersion.
func (dv DataVersion) IntoProto() *viewpb.DataVersion {
	return &viewpb.DataVersion{
		StreamingVersion: dv.StreamingVersion,
		CompactVersion:   dv.CompactVersion,
	}
}

// QueryViewKey uniquely identifies a query view by shard and version.
type QueryViewKey struct {
	ShardID          ShardID
	QueryViewVersion QueryViewVersion
}

// String returns the string representation of the query view key.
func (k QueryViewKey) String() string {
	return fmt.Sprintf("%s-%s", k.ShardID, k.QueryViewVersion)
}

// QueryViewVersion is the composite version of a query view.
// Ordered lexicographically by (DataVersion, QueryVersion).
type QueryViewVersion struct {
	DataVersion  DataVersion
	QueryVersion int64
}

// String returns the string representation of the query view version.
func (qv QueryViewVersion) String() string {
	return fmt.Sprintf("%s/%d", qv.DataVersion.String(), qv.QueryVersion)
}

// EQ returns true if qv is equal to other.
func (qv QueryViewVersion) EQ(other QueryViewVersion) bool {
	return qv.DataVersion.EQ(other.DataVersion) && qv.QueryVersion == other.QueryVersion
}

// GT returns true if qv is strictly greater than other (lexicographic).
func (qv QueryViewVersion) GT(other QueryViewVersion) bool {
	if !qv.DataVersion.EQ(other.DataVersion) {
		return qv.DataVersion.GT(other.DataVersion)
	}
	return qv.QueryVersion > other.QueryVersion
}

// GTE returns true if qv is greater than or equal to other.
func (qv QueryViewVersion) GTE(other QueryViewVersion) bool {
	return qv.EQ(other) || qv.GT(other)
}

// FromProtoQueryViewVersion converts a QueryViewVersion proto to a QueryViewVersion.
func FromProtoQueryViewVersion(qvv *viewpb.QueryViewVersion) QueryViewVersion {
	return QueryViewVersion{
		DataVersion:  FromProtoDataVersion(qvv.DataVersion),
		QueryVersion: qvv.QueryVersion,
	}
}

// IntoProto converts a QueryViewVersion to a proto QueryViewVersion.
func (qv QueryViewVersion) IntoProto() *viewpb.QueryViewVersion {
	return &viewpb.QueryViewVersion{
		DataVersion:  qv.DataVersion.IntoProto(),
		QueryVersion: qv.QueryVersion,
	}
}
