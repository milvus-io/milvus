package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type constructorStreamFactory struct {
	qnview.SegmentLoadInfoStreamFactory
}

func (*constructorStreamFactory) NewSegmentLoadInfoStream(context.Context) qnview.SegmentLoadInfoStream {
	panic("mockey")
}

func TestNewQueryViewSegmentManagerDependencies(t *testing.T) {
	paramtable.Init()
	for _, missing := range []string{
		"load resource budget", "chunk manager", "load metadata provider",
		"TransformLog stream manager", "SegmentLoadInfoStreamFactory", "nil stream", "",
	} {
		t.Run(missing, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			budget := &segments.LoadResourceBudget{}
			var cm storage.ChunkManager = struct{ storage.ChunkManager }{}
			var meta qnview.QueryViewLoadMetadataProvider = struct {
				qnview.QueryViewLoadMetadataProvider
			}{}
			var streams wal.TransformLogStreamManager = struct{ wal.TransformLogStreamManager }{}
			var factory qnview.SegmentLoadInfoStreamFactory = &constructorStreamFactory{}
			var stream qnview.SegmentLoadInfoStream = struct{ qnview.SegmentLoadInfoStream }{}
			switch missing {
			case "load resource budget":
				budget = nil
			case "chunk manager":
				cm = nil
			case "load metadata provider":
				meta = nil
			case "TransformLog stream manager":
				streams = nil
			case "SegmentLoadInfoStreamFactory":
				factory = nil
			case "nil stream":
				stream = nil
			}
			factoryMock := mockey.Mock((*constructorStreamFactory).NewSegmentLoadInfoStream).To(
				func(_ *constructorStreamFactory, got context.Context) qnview.SegmentLoadInfoStream {
					require.Same(t, ctx, got)
					return stream
				}).Build()
			defer factoryMock.UnPatch()
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			schedulerMock := mockey.Mock(nodescheduler.Get).Return(scheduler).Build()
			defer schedulerMock.UnPatch()

			manager, err := NewQueryViewSegmentManager(ctx, budget, cm, meta, streams, factory)
			if missing == "" {
				require.NoError(t, err)
				require.NotNil(t, manager)
				require.Equal(t, 1, factoryMock.Times())
				require.Equal(t, 1, schedulerMock.Times())
				return
			}
			require.Nil(t, manager)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.ErrorContains(t, err, missing)
			require.Zero(t, schedulerMock.Times(), "dependency failure must precede scheduler initialization")
			if missing == "nil stream" {
				require.Equal(t, 1, factoryMock.Times())
			} else {
				require.Zero(t, factoryMock.Times(), "validate dependencies before opening the stream")
			}
		})
	}
}
