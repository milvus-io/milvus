package transformlogbuffer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// Exercise both real catchup/dispatch implementations together. Only the stream
// acquisition and physical Segment are replaced; no fake cursor advances either
// SN or QN across a missing Delete.
func TestDispatchPipelinePreservesDeletesAcrossBothHandoffs(t *testing.T) {
	mockey.PatchConvey("history and live suffix reach every physical registration exactly once", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		manager := walsummary.NewManager(walsummary.ManagerConfig{})
		observe := func(tick uint64) {
			msg := message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").
				WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
				WithBody(&msgpb.DeleteRequest{
					CollectionID: 1, PartitionID: 1,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(tick)}}}},
					Timestamps:  []uint64{tick},
				}).MustBuildMutable().WithTimeTick(tick).
				WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
			manager.ObserveMessage(ctx, msg)
		}
		for i := uint64(1); i <= 400; i++ {
			observe(i)
		}
		stream := walsummary.NewDispatchStream(manager)
		defer stream.Close()
		streams := newFakeStreamManager()
		mockey.Mock((*fakeStreamManager).AcquireStream).Return(stream, nil).Build()
		var readOriginal func(*walsummary.Manager, context.Context, string, uint64, uint64, walsummary.ReadLimits) (walsummary.TransformBatch, error)
		var reads atomic.Int32
		mockey.Mock((*walsummary.Manager).ReadTransform).Origin(&readOriginal).To(func(m *walsummary.Manager, ctx context.Context, vc string, after, through uint64, limits walsummary.ReadLimits) (walsummary.TransformBatch, error) {
			reads.Add(1)
			return readOriginal(m, ctx, vc, after, through, limits)
		}).Build()
		buffer := New(streams, 1)
		guard, err := buffer.Acquire(ctx, newTestQueryView("p_1v0", 0))
		require.NoError(t, err)
		defer guard.Release()
		require.NoError(t, guard.WaitTransformVisible(ctx, 400))

		entered, resume := make(chan struct{}), make(chan struct{})
		var release sync.Once
		defer release.Do(func() { close(resume) })
		var applyOriginal func(*fakeSegment, context.Context, *streamingpb.TransformLogEntry) error
		mockey.Mock((*fakeSegment).ApplyTransform).Origin(&applyOriginal).To(func(s *fakeSegment, ctx context.Context, entry *streamingpb.TransformLogEntry) error {
			if entry.TimeTick == 1 {
				close(entered)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return applyOriginal(s, ctx, entry)
		}).Build()
		segment := &fakeSegment{id: 1, vchannel: "p_1v0"}
		reg, err := buffer.RegisterSegment(ctx, segment)
		require.NoError(t, err)
		defer reg.Unregister()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("segment replay did not start")
		}
		for i := uint64(401); i <= 700; i++ {
			observe(i)
		}
		manager.ObserveQueryBarrier("p_1v0", 701)
		require.NoError(t, guard.WaitTransformVisible(ctx, 701))
		release.Do(func() { close(resume) })
		require.NoError(t, reg.WaitCatchup(ctx))
		require.NoError(t, reg.WaitTransformVisible(ctx, 701))
		for i := uint64(702); i <= 1000; i++ {
			observe(i)
		}
		manager.ObserveQueryBarrier("p_1v0", 1001)
		require.NoError(t, reg.WaitTransformVisible(ctx, 1001))
		want := make([]uint64, 0, 999)
		for i := uint64(1); i <= 1000; i++ {
			if i != 701 {
				want = append(want, i)
			}
		}
		require.Equal(t, want, segment.appliedTicks())
		require.EqualValues(t, 1, reads.Load(), "SN live dispatch must never reread Summary")
	})
}
