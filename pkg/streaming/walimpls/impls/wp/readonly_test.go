package wp

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestReadOnlyOpenPreservesWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pt := paramtable.Get()
	require.NoError(t, pt.Save(pt.WoodpeckerCfg.StorageType.Key, "local"))
	require.NoError(t, pt.Save(pt.WoodpeckerCfg.RootPath.Key, t.TempDir()))
	defer pt.Reset(pt.WoodpeckerCfg.StorageType.Key)
	defer pt.Reset(pt.WoodpeckerCfg.RootPath.Key)
	o, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	defer func() { o.Close(); require.NoError(t, woodpecker.StopEmbedLogStore()) }()
	opt := &walimpls.OpenOption{Channel: types.PChannelInfo{Name: uniqueWpPChannel("auto-tail"), Term: 1, AccessMode: types.AccessModeRW}}
	w, err := o.Open(ctx, opt)
	require.NoError(t, err)
	first, err := w.Append(ctx, message.CreateTestEmptyInsertMesage(1, nil))
	require.NoError(t, err)
	roOpt := *opt
	roOpt.Channel.AccessMode = types.AccessModeRO
	reader, err := o.Open(ctx, &roOpt)
	require.NoError(t, err)
	require.Nil(t, reader.(*walImpl).p, "historical scanner must not acquire/fence a writer")
	s, err := reader.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
	require.NoError(t, err)
	defer s.Close()
	reader.Close()
	second, err := w.Append(ctx, message.CreateTestEmptyInsertMesage(2, nil))
	require.NoError(t, err, "opening/closing read-only handle cannot fence the active writer")
	w.Close()
	w, err = o.Open(ctx, opt)
	require.NoError(t, err)
	defer w.Close()
	third, err := w.Append(ctx, message.CreateTestEmptyInsertMesage(3, nil))
	require.NoError(t, err)
	for _, id := range []message.MessageID{first, second, third} {
		select {
		case msg, ok := <-s.Chan():
			require.True(t, ok, "retiring the RO owner must not interrupt its scanner")
			require.True(t, id.EQ(msg.MessageID()))
		case <-ctx.Done():
			t.Fatal("historical scanner stalled after RO close and writer replacement")
		}
	}
	require.NoError(t, s.Close())
	_, err = w.Append(ctx, message.CreateTestEmptyInsertMesage(4, nil))
	require.NoError(t, err, "releasing the retired RO log cannot fence the replacement writer")
	// Metadata and fencing failures must not yield a usable handle.
	client := o.(*openerImpl).c
	patch := mockey.Mock(mockey.GetMethod(client, "LogExists")).Return(false, context.DeadlineExceeded).Build()
	_, err = o.Open(ctx, opt)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	patch.UnPatch()
	exists := mockey.Mock(mockey.GetMethod(client, "LogExists")).Return(false, nil).Build()
	patch = mockey.Mock(mockey.GetMethod(client, "CreateLog")).Return(context.DeadlineExceeded).Build()
	_, err = o.Open(ctx, opt)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	patch.UnPatch()
	exists.UnPatch()
	patch = mockey.Mock(mockey.GetMethod(client, "OpenLog")).Return(nil, context.DeadlineExceeded).Build()
	_, err = o.Open(ctx, opt)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	patch.UnPatch()
	patch = mockey.Mock(mockey.GetMethod(w.(*walImpl).l, "OpenLogWriter")).Return(nil, context.DeadlineExceeded).Build()
	_, err = o.Open(ctx, opt)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	patch.UnPatch()
}
