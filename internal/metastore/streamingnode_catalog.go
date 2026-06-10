package metastore

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// StreamingNodeCataLog is the interface for streamingnode catalog
type StreamingNodeCataLog interface {
	// WAL select the wal related recovery infos.
	// Which must give the pchannel name.

	// ListVChannel list all vchannels on current pchannel.
	ListVChannel(ctx context.Context, pchannelName string) ([]*streamingpb.VChannelMeta, error)

	// SaveVChannels save vchannel on current pchannel.
	SaveVChannels(ctx context.Context, pchannelName string, vchannels map[string]*streamingpb.VChannelMeta) error

	// DropVChannels drops retained vchannel recovery meta on current pchannel.
	DropVChannels(ctx context.Context, pchannelName string, vchannels map[string]*streamingpb.VChannelMeta) error

	// ListTransformLogMeta lists transform log metas on current pchannel.
	ListTransformLogMeta(ctx context.Context, pchannelName string) (map[string]*streamingpb.VChannelTransformLogMeta, error)

	// SaveTransformLogMeta saves transform log metas on current pchannel.
	SaveTransformLogMeta(ctx context.Context, pchannelName string, metas map[string]*streamingpb.VChannelTransformLogMeta) error

	// DropTransformLogMeta drops transform log metas on current pchannel.
	DropTransformLogMeta(ctx context.Context, pchannelName string, vchannels []string) error

	// ListSegmentAssignment list all segment assignments for the wal.
	ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error)

	// ListQueryViews lists persisted StreamingNode query views for the WAL.
	ListQueryViews(ctx context.Context, pChannelName string) ([]*viewpb.QueryViewOfShard, error)

	// SaveQueryViews persists Up views and removes non-Up recovery records.
	SaveQueryViews(ctx context.Context, pChannelName string, views []*viewpb.QueryViewOfShard) error
	// SaveSegmentAssignments save the segment assignments for the wal.
	SaveSegmentAssignments(ctx context.Context, pChannelName string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error

	// DropSegmentAssignments drops retained segment assignment recovery meta for the wal.
	DropSegmentAssignments(ctx context.Context, pChannelName string, segmentIDs []int64) error
	// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
	// Return nil, nil if the checkpoint is not exist.
	GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error)

	// SaveConsumeCheckpoint saves the consuming checkpoint of the wal.
	SaveConsumeCheckpoint(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint) error

	// GetSalvageCheckpoint gets all salvage checkpoints for a channel.
	// Returns an empty slice if none exist. One checkpoint per source cluster.
	GetSalvageCheckpoint(ctx context.Context, pChannelName string) ([]*commonpb.ReplicateCheckpoint, error)

	// SaveRecoverySnapshot applies a WAL recovery DELTA in one compound
	// operation. Despite the name it is not a full-state replacement: only the
	// entries present in the payload are touched, missing keys are left
	// unchanged, and closed/tombstoned recovery metadata is retained until an
	// explicit DropVChannels or DropSegmentAssignments cleanup. It therefore
	// cannot express "replace the persisted recovery state with this set" (in
	// particular an empty payload is a no-op, not a wipe).
	//
	// The etcd-based implementation stages the delta as a single composite
	// write via the shared txn.Builder/txn.Commit primitive - atomically when
	// the op count fits the etcd txn limit, else via an ordered chunked
	// fallback - with the consume checkpoint always the last/commit-marker op.
	// On the atomic path a crash before the checkpoint lands makes the whole
	// delta invisible and the next retry re-persists everything (every part is
	// an idempotent put on a deterministic key). On the fallback path the
	// non-commit parts are visible immediately; the checkpoint-last ordering
	// still holds, but the whole-delta invisibility guarantee does not. A CAS
	// single-point commit (see the recovery background-task TODO) is the
	// durable fix.
	SaveRecoverySnapshot(ctx context.Context, pChannelName string, snapshot *WALRecoverySnapshot) error
}

// WALRecoverySnapshot is the compound payload of
// StreamingNodeCataLog.SaveRecoverySnapshot. It is a delta, not a full
// snapshot: absent sections mean "unchanged", and cleanup is handled by the
// explicit catalog drop methods. See SaveRecoverySnapshot.
type WALRecoverySnapshot struct {
	// SegmentAssignments are the segment assignments to save; skipped if empty.
	SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
	// VChannels are the vchannel metas to save; skipped if empty.
	VChannels map[string]*streamingpb.VChannelMeta
	// SalvageCheckpoint is the salvage checkpoint to save; skipped if nil.
	// It must be persisted before the consume checkpoint to guarantee ordering.
	SalvageCheckpoint *commonpb.ReplicateCheckpoint
	// ConsumeCheckpoint is the consume checkpoint to save; skipped if nil.
	// It is always written last as the commit point of the snapshot.
	ConsumeCheckpoint *streamingpb.WALCheckpoint
}
