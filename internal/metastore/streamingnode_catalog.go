package metastore

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// StreamingNodeCataLog is the interface for streamingnode catalog
type StreamingNodeCataLog interface {
	// WAL select the wal related recovery infos.
	// Which must give the pchannel name.

	// ListVChannel list all vchannels on current pchannel.
	ListVChannel(ctx context.Context, pchannelName string) ([]*streamingpb.VChannelMeta, error)

	// SaveVChannels save vchannel on current pchannel.
	SaveVChannels(ctx context.Context, pchannelName string, vchannels map[string]*streamingpb.VChannelMeta) error

	// ListSegmentAssignment list all segment assignments for the wal.
	ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error)

	// SaveSegmentAssignments save the segment assignments for the wal.
	SaveSegmentAssignments(ctx context.Context, pChannelName string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error

	// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
	// Return nil, nil if the checkpoint is not exist.
	GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error)

	// SaveConsumeCheckpoint saves the consuming checkpoint of the wal.
	SaveConsumeCheckpoint(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint) error

	// SaveSalvageCheckpoint saves the salvage checkpoint.
	// The checkpoint is captured during force promote.
	SaveSalvageCheckpoint(ctx context.Context, pChannelName string, checkpoint *commonpb.ReplicateCheckpoint) error

	// GetSalvageCheckpoint gets all salvage checkpoints for a channel.
	// Returns an empty slice if none exist. One checkpoint per source cluster.
	GetSalvageCheckpoint(ctx context.Context, pChannelName string) ([]*commonpb.ReplicateCheckpoint, error)

	// SaveRecoverySnapshot saves a WAL recovery snapshot in one compound
	// operation. Nil or empty parts of the snapshot are skipped. The etcd-based
	// implementation writes the parts in order: segment assignments, vchannels,
	// salvage checkpoint, and strictly last the consume checkpoint, which is
	// the commit point of the snapshot — a crash before it is written makes the
	// whole snapshot invisible and the next retry re-persists everything (all
	// parts are idempotent puts on deterministic keys). A future atomic
	// implementation can persist the whole snapshot in a single
	// compare-and-swap round-trip (see the CAS TODO in the recovery storage).
	SaveRecoverySnapshot(ctx context.Context, pChannelName string, snapshot *WALRecoverySnapshot) error
}

// WALRecoverySnapshot is the compound payload of
// StreamingNodeCataLog.SaveRecoverySnapshot.
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
