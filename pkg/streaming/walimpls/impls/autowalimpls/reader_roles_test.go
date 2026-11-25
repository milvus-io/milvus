package autowalimpls

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

type trackedReadOnlyLog struct{ *readOnlyLog }

func (*trackedReadOnlyLog) Close() { panic("mockey") }

func TestCurrentWriterAndHistoricalReaders(t *testing.T) {
	mockey.PatchConvey("one historical handle per backend preserves existing scanners", t, func() {
		p, w, _ := installBackends(t)
		ctx := context.Background()
		first, err := walimpls.WALImpls(w).Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		ro := &trackedReadOnlyLog{&readOnlyLog{w}}
		roClosed := 0
		closed := map[*physicalLog]int{}
		mockey.Mock((*trackedReadOnlyLog).Close).To(func(*trackedReadOnlyLog) { roClosed++ }).Build()
		mockey.Mock((*physicalLog).Close).To(func(b *physicalLog) { closed[b]++ }).Build()
		var modes []types.AccessMode
		failRW := true
		auto := New(p, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			require.Equal(t, w.name, name)
			modes = append(modes, mode)
			if mode == types.AccessModeRO {
				return ro, nil
			}
			if failRW {
				failRW = false
				return nil, context.DeadlineExceeded
			}
			return w, nil
		})
		defer auto.Close()
		s, err := auto.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		require.EqualValues(t, 10, receive(t, s).TimeTick())
		require.Same(t, p, auto.current, "historical reads do not select the writer")
		require.Same(t, ro, auto.readers[w.name])

		require.ErrorIs(t, auto.SwitchWriterForRecovery(ctx, w.name), context.DeadlineExceeded)
		require.Same(t, p, auto.current)
		require.Same(t, ro, auto.readers[w.name], "a failed RW open must retain the historical reader")
		require.Zero(t, roClosed)
		require.NoError(t, auto.SwitchWriterForRecovery(ctx, w.name))
		require.Same(t, w, auto.current)
		require.Same(t, p, auto.readers[p.name])
		require.NotContains(t, auto.readers, w.name)
		require.Equal(t, 1, roClosed, "the replaced RO handle is released without closing its scanner")
		_, err = auto.Append(ctx, ordinary(20, first))
		require.NoError(t, err)
		require.EqualValues(t, 20, receive(t, s).TimeTick())

		require.NoError(t, auto.SwitchWriterForRecovery(ctx, p.name))
		require.Same(t, p, auto.current)
		require.NotContains(t, auto.readers, p.name)
		require.Same(t, w, auto.readers[w.name])
		reader, err := auto.backend(ctx, w.name, false)
		require.NoError(t, err)
		require.Same(t, w, reader, "retired RW handles can serve history and truncation")
		require.NoError(t, auto.Truncate(ctx, first))
		require.True(t, first.EQ(w.truncated))

		require.NoError(t, auto.SwitchWriterForRecovery(ctx, w.name))
		require.Same(t, w, auto.current)
		require.Equal(t, []types.AccessMode{types.AccessModeRO, types.AccessModeRW, types.AccessModeRW}, modes, "round trips reuse retired RW handles")
		require.Empty(t, closed)
		require.NoError(t, s.Close())
		auto.Close()
		auto.Close()
		require.Equal(t, map[*physicalLog]int{p: 1, w: 1}, closed)
		require.Equal(t, 1, roClosed)
	})
}
