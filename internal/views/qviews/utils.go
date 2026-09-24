package qviews

import (
	"context"
	"fmt"
	"strings"

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
	return strings.TrimPrefix(viewpb.QueryViewState(s).String(), "QueryViewState")
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

// FromProtoShardID converts a proto ShardID to a domain ShardID.
func FromProtoShardID(pb *viewpb.ShardID) ShardID {
	return ShardID{
		ReplicaID: pb.ReplicaId,
		VChannel:  pb.Vchannel,
	}
}

// IntoProto converts a ShardID to a proto ShardID.
func (id ShardID) IntoProto() *viewpb.ShardID {
	return &viewpb.ShardID{
		ReplicaId: id.ReplicaID,
		Vchannel:  id.VChannel,
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
	return fmt.Sprintf("%d/%d", dv.StreamingVersion, dv.CompactVersion)
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
		StreamingVersion: dv.GetStreamingVersion(),
		CompactVersion:   dv.GetCompactVersion(),
	}
}

// IntoProto converts a DataVersion to a proto DataVersion.
func (dv DataVersion) IntoProto() *viewpb.DataVersion {
	return &viewpb.DataVersion{
		StreamingVersion: dv.StreamingVersion,
		CompactVersion:   dv.CompactVersion,
	}
}

// SegmentStats is the per-segment load footprint published by the DataView
// Manager for one DataView version. It currently carries only the segment
// RowNum; future metrics (e.g. MemSize) extend this struct without changing
// the map shape or the DataViewRef access contract.
type SegmentStats struct {
	RowNum int64
}

// DataViewRef is a read-only reference to one DataView version. The Manager
// ref-counts the referenced version against collection-scoped GC, so a
// consumer may safely hold the ref until Deref.
//
// PRECONDITION (immutable): the caller must not mutate the returned
// DataView / SegmentStats data. The referenced structures are shared,
// read-only snapshots; modification corrupts the Manager's state.
type DataViewRef interface {
	// DataView returns the referenced proto DataView snapshot.
	DataView() *viewpb.DataViewOfCollection
	// Version returns the DataVersion of the referenced DataView.
	Version() *viewpb.DataVersion
	// Stats returns the published SegmentStats of one segment. ok is false
	// when the segment has no published footprint in this version.
	Stats(segmentID int64) (SegmentStats, bool)
	// Deref releases the reference. Idempotent; each acquirer must call it
	// exactly once when the ref is no longer needed.
	Deref()
}

// DataViewRefProvider acquires DataViewRefs for QueryViews. It is implemented
// by the DataView Manager (internal/dataview.Manager satisfies it directly,
// so the wiring layer injects the Manager as-is). The QueryView lifecycle
// holds the acquired ref (lifetime(QueryView) < lifetime(DataView)) and
// releases it with Deref when the view is durably removed.
type DataViewRefProvider interface {
	// Get acquires a ref to the DataView at the exact DataVersion. It
	// returns (nil, nil) when the version does not exist (e.g. already GC'd),
	// and a non-nil error on provider failure.
	Get(ctx context.Context, collectionID int64, version *viewpb.DataVersion) (DataViewRef, error)
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

// UnknownReplicaID requests resolution of the replica from its vchannel.
const UnknownReplicaID int64 = 0
