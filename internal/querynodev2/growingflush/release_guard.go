// Package growingflush holds the QueryNode-side guards that keep a growing
// segment alive while the local write buffer still owes a growing-source flush
// for it.
//
// With growing-source flush enabled the DataNode's write buffer does NOT buffer
// the inserts of such a segment (l0_write_buffer skips bufferInsert for
// FlushSourceGrowing segments); it only keeps a progress ledger and pulls the
// rows from the QueryNode growing segment when the flush runs. The QueryNode
// growing segment is therefore the ONLY in-memory copy of those rows, the
// source decision is sticky and irreversible, and the debt is cleared only by a
// successful flush. Dropping such a segment while the debt is outstanding is
// not self-healing: the flush can never resolve a source again and the channel
// checkpoint stays pinned behind it until the process restarts.
package growingflush

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// IsPrepareUnavailable reports whether the release-handoff prepare failed
// because no preparer could be reached, as opposed to failing the prepare
// itself.
func IsPrepareUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, merr.ErrServiceUnavailable) ||
		errors.Is(err, merr.ErrChannelNotAvailable) ||
		errors.Is(err, handler.ErrClientClosed) ||
		errors.Is(err, handler.ErrReadOnlyWAL) ||
		errors.Is(err, registry.ErrNoStreamingNodeDeployed) ||
		errors.Is(err, registry.ErrNoReleaseManualFlushPreparer) {
		return true
	}
	streamingErr := streamingstatus.AsStreamingError(err)
	return streamingErr.IsOnShutdown() || streamingErr.IsWrongStreamingNode()
}

// IsPrepareStructurallyUnavailable reports that no LOCAL write buffer can be
// left owing a growing-source flush for this channel, so skipping the
// release-handoff prepare cannot strand one:
//
//   - no streaming node / no preparer in this process: growing-source flush is
//     process-local (the source registry is a process singleton), so a remote
//     write buffer never resolves this node's growing segments;
//   - channel not available / served by another streaming node: same reasoning,
//     the channel's write buffer is not in this process;
//   - WAL shutting down: the local write buffer closes with it, its progress
//     dies with the instance, and the next WAL owner replays from the
//     unadvanced checkpoint.
//
// Everything else unavailable-shaped (service unavailable, client closed,
// read-only WAL) is transient: the local write buffer may be alive and stay
// alive, so the caller must NOT skip the drain.
//
// In particular merr.ErrChannelNotFound must NEVER be added here, however much
// "channel not found ⇒ nothing local can owe a flush" reads like a structural
// case. It is not: bufferManager.DropChannel does GetAndRemove(channel) FIRST
// and only then runs the long final buf.Close(dropCtx, true). During that
// detach window the write buffer is gone from the manager's map — so
// bufferManager.GetGrowingFlushProgress answers merr.WrapErrChannelNotFound —
// while the buffer object is still very much alive and may still owe a
// growing-source flush for segments whose only in-memory copy is this node's
// growing segments. Classifying it structural would let the release path drop
// those segments inside that window and strand the flush forever. Not-found is
// therefore transient by construction: the caller refuses and retries, and by
// then the drop has either completed (buffer really gone, nothing owed) or the
// buffer is visible again.
//
// See TestChannelNotFoundIsNeverStructural, which fails if this is ever
// relaxed.
func IsPrepareStructurallyUnavailable(err error) bool {
	if errors.Is(err, registry.ErrNoStreamingNodeDeployed) ||
		errors.Is(err, registry.ErrNoReleaseManualFlushPreparer) ||
		errors.Is(err, merr.ErrChannelNotAvailable) {
		return true
	}
	streamingErr := streamingstatus.AsStreamingError(err)
	return streamingErr.IsOnShutdown() || streamingErr.IsWrongStreamingNode()
}

// CheckReleaseDebt guards a PARTIAL release of growing segments — a
// ReleaseSegments with DataScope_Streaming / DataScope_All, issued by
// ReleasePartitions or a target update while the channel stays subscribed.
//
// Unlike the channel-release path (UnsubDmChannel) it must not fence the
// channel and must not block on the drain, because the channel survives:
//
//   - "nudge + refuse-retry". If any of these segments still owes a
//     growing-source flush, the flush is driven forward (a ManualFlush is
//     appended by the local preparer) and a RETRYABLE error is returned without
//     removing anything. The coordinator retries; by then the debt is normally
//     settled and the retry removes cleanly.
//   - No channel-wide admission fence. FenceGrowingSourceAdmission is per write
//     buffer (per channel) and only reopens when a NEWER growing-source provider
//     registration appears, i.e. on re-subscription — using it here would
//     permanently degrade the surviving partitions of a live channel to
//     write-buffer mode.
//
// It returns nil (proceed with the removal, today's behavior) whenever no local
// write buffer can owe a flush for these segments.
func CheckReleaseDebt(ctx context.Context, collectionID int64, vchannel string, growingSegmentIDs []int64) error {
	segmentIDs := lo.Uniq(lo.Filter(growingSegmentIDs, func(segmentID int64, _ int) bool {
		return segmentID > 0
	}))
	if len(segmentIDs) == 0 || vchannel == "" || collectionID == 0 {
		return nil
	}
	// No local growing-source provider for this channel means no local write
	// buffer ever chose FlushSourceGrowing here, so nothing can be owed.
	if syncmgr.DefaultGrowingSourceRegistry().ProviderCount(vchannel) == 0 {
		return nil
	}
	wal := streaming.WAL()
	if wal == nil {
		// Streaming was never initialized in this process, so there is no local
		// write buffer to owe anything — same reasoning as the structural cases.
		return nil
	}

	pending, err := wal.Local().PrepareReleaseSegmentsIfLocal(ctx, collectionID, vchannel, segmentIDs)
	if err != nil {
		if IsPrepareStructurallyUnavailable(err) {
			mlog.Warn(ctx, "growing-source release check structurally unavailable, release growing segments anyway",
				mlog.String("channel", vchannel),
				mlog.Int64("collectionID", collectionID),
				mlog.Int64s("segmentIDs", segmentIDs),
				mlog.Err(err))
			return nil
		}
		// Transient (or a real failure): the local write buffer may be alive and
		// owing a flush for these segments. Refuse the release so it is retried
		// rather than dropping the only copy of the rows.
		mlog.Warn(ctx, "growing-source release check failed, refuse to release growing segments",
			mlog.String("channel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("segmentIDs", segmentIDs),
			mlog.Err(err))
		return err
	}
	if pending {
		mlog.Warn(ctx, "growing-source flush still owed for released segments, refuse the release for retry",
			mlog.String("channel", vchannel),
			mlog.Int64("collectionID", collectionID),
			mlog.Int64s("segmentIDs", segmentIDs))
		return merr.WrapErrServiceUnavailable(
			"growing-source flush still owed for released segments, retry later",
			"ReleaseSegments")
	}
	return nil
}
