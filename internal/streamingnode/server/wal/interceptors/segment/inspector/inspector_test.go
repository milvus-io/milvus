package inspector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/segment/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestSealedInspector(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	notifier := stats.NewSealSignalNotifier()
	inspector := NewSealedInspector(notifier)

	o := mock_inspector.NewMockSealOperator(t)
	ops := atomic.NewInt32(0)

	o.EXPECT().Channel().Return(types.PChannelInfo{Name: "v1"})
	o.EXPECT().TryToSealSegments(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, sb ...stats.SegmentBelongs) {
			ops.Add(1)
		})
	o.EXPECT().TryToSealWaitedSegment(mock.Anything).
		RunAndReturn(func(ctx context.Context) {
			ops.Add(1)
		})
	o.EXPECT().IsNoWaitSeal().RunAndReturn(func() bool {
		return ops.Load()%2 == 0
	})

	inspector.RegsiterPChannelManager(o)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			inspector.TriggerSealWaited(context.Background(), "v1")
			ops.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			notifier.AddAndNotify(stats.SegmentBelongs{
				PChannel:     "v1",
				VChannel:     "vv1",
				CollectionID: 12,
				PartitionID:  1,
				SegmentID:    2,
			})
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
	}()
	wg.Wait()
	inspector.UnregisterPChannelManager(o)
	inspector.Close()
}
