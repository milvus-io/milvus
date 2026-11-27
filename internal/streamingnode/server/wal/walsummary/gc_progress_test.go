package walsummary

import (
	"context"
	"io/fs"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPrefixGCWaitsOnlyForPreTruncateReaders(t *testing.T) {
	for _, kind := range []string{"transform", "idempotency entries", "idempotency recovery"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				m, store := newTestManagerWithStore(t)
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 99, 1, 1))
				require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
				require.NoError(t, drainSummary(ctx, m))
				entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
				release := []chan struct{}{make(chan struct{}), make(chan struct{})}
				unblockOld := sync.OnceFunc(func() { close(release[0]) })
				unblockNew := sync.OnceFunc(func() { close(release[1]) })
				var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry, *chunkValidation) (*chunkPayload, error)
				patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(s *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
					index = proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
					close(entered[index.Generation])
					<-release[index.Generation]
					return original(s, ctx, index, verified)
				}).Build()
				defer patch.UnPatch()
				defer unblockOld()
				defer unblockNew()
				read := func(from, to uint64) error {
					switch kind {
					case "transform":
						_, err := m.ReadTransform(ctx, "v1", from, to, ReadLimits{})
						return err
					case "idempotency entries":
						_, err := m.ReadIdempotencyEntries(ctx, "v1", from, to)
						return err
					default:
						_, err := m.ReadIdempotencyRecovery(ctx, idempotencyview.RetentionConfig{MaxSize: 1}, time.Now())
						return err
					}
				}
				oldDone, newDone := make(chan error, 1), make(chan error, 1)
				go func() { oldDone <- read(0, 100) }()
				<-entered[0]
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 199, 1, 2))
				require.NoError(t, stageChunk(t, m, 200).Execute(ctx))
				require.NoError(t, drainSummary(ctx, m))
				m.SetDataViewRetention("v1", 200)
				m.cfg.MaxRetentionBytes = m.Manifest().Chunks[1].ObjectSize
				require.NoError(t, m.GCOnce(ctx))
				require.ErrorIs(t, m.gcTask.Execute(ctx), nodescheduler.ErrDelay)
				require.NoError(t, m.manifestTask.Execute(ctx))
				go func() { newDone <- read(100, 200) }()
				<-entered[1]
				require.ErrorIs(t, m.gcTask.Execute(ctx), nodescheduler.ErrDelay, "old snapshot still owns the prefix")
				unblockOld()
				require.NoError(t, <-oldDone)
				require.NoError(t, m.gcTask.Execute(ctx), "new suffix readers must not starve deletion")
				exists, err := store.chunkManager.Exist(ctx, store.ChunkKey(0))
				require.NoError(t, err)
				require.False(t, exists)
				// The same suffix reader must protect the next truncate, however.
				m.cfg.MaxRetentionBytes = 0
				require.NoError(t, m.GCOnce(ctx))
				require.NoError(t, m.manifestTask.Execute(ctx))
				require.ErrorIs(t, m.gcTask.Execute(ctx), nodescheduler.ErrDelay)
				unblockNew()
				require.NoError(t, <-newDone)
				require.NoError(t, m.gcTask.Execute(ctx))
				require.Empty(t, m.Manifest().Chunks)
			})
		})
	}
}

func TestPrefixGCProgressesWhileManifestKeepsAdvancing(t *testing.T) {
	ctx := t.Context()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, stageChunk(t, m, 200).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	m.cfg.MaxRetentionBytes = m.Manifest().Chunks[1].ObjectSize
	require.NoError(t, m.GCOnce(ctx))
	require.ErrorIs(t, m.gcTask.Execute(ctx), nodescheduler.ErrDelay)
	var original func(*Store, context.Context, *streamingpb.PChannelSummaryManifest) error
	patch := mockey.Mock((*Store).WriteManifest).Origin(&original).To(func(s *Store, ctx context.Context, manifest *streamingpb.PChannelSummaryManifest) error {
		require.NoError(t, stageChunk(t, m, 300).Execute(ctx))
		return original(s, ctx, manifest)
	}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, m.manifestTask.Execute(ctx), nodescheduler.ErrDelay)
	require.Less(t, m.publishedVersion, m.manifestVersion)
	// Even another publication in flight cannot revive the retired prefix.
	m.publishMu.Lock()
	err := m.gcTask.Execute(ctx)
	m.publishMu.Unlock()
	require.NoError(t, err)
	require.True(t, m.gcTask.Done())
	for gen := uint64(0); gen < 3; gen++ {
		exists, err := store.chunkManager.Exist(ctx, store.ChunkKey(gen))
		require.NoError(t, err)
		require.Equal(t, gen != 0, exists)
	}
}

func TestGCDeleteMissingObjectsIsIdempotent(t *testing.T) {
	for _, missing := range []error{fs.ErrNotExist, merr.WrapErrIoKeyNotFound("gone")} {
		t.Run(missing.Error(), func(t *testing.T) {
			ctx := t.Context()
			m, store := newTestManagerWithStore(t)
			require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
			require.NoError(t, drainSummary(ctx, m))
			m.cfg.MaxRetentionBytes = 0
			require.NoError(t, m.GCOnce(ctx))
			require.NoError(t, m.manifestTask.Execute(ctx))
			var original func(*storage.LocalChunkManager, context.Context, string) error
			calls := 0
			patch := mockey.Mock((*storage.LocalChunkManager).Remove).Origin(&original).To(func(cm *storage.LocalChunkManager, ctx context.Context, key string) error {
				calls++
				if calls == 1 {
					require.NoError(t, original(cm, ctx, key))
					return context.DeadlineExceeded // server deleted; client lost the response
				}
				return missing
			}).Build()
			defer patch.UnPatch()
			requireSummaryError(t, m.gcTask.Execute(ctx), nodescheduler.ErrDelay)
			require.NoError(t, m.gcTask.Execute(ctx))
			require.True(t, m.gcTask.Done())
			// Already-gone files in the old-manifest listing also count as done.
			key := buildManifestPrefix(store.chunkManager, store.PChannel()) + "0"
			require.NoError(t, store.chunkManager.Write(ctx, key, []byte("old")))
			old := time.Now().Add(-48 * time.Hour)
			require.NoError(t, os.Chtimes(key, old, old))
			require.NoError(t, store.DeleteManifestsBelowTerm(ctx, 1, time.Now().Add(-24*time.Hour)))
			patch.UnPatch()
			require.NoError(t, stageChunk(t, m, 200).Execute(ctx))
			require.NoError(t, gcSummary(ctx, m), "next prefix must still make progress")
		})
	}
}
