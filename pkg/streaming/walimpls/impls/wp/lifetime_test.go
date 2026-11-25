package wp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/woodpecker"
	wp "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type lifetimeClient struct{ woodpecker.Client }

func (*lifetimeClient) LogExists(context.Context, string) (bool, error) { panic("mockey") }
func (*lifetimeClient) OpenLog(context.Context, string) (wp.LogHandle, error) {
	panic("mockey")
}

type lifetimeLog struct{ wp.LogHandle }

func (*lifetimeLog) OpenLogWriter(context.Context) (wp.LogWriter, error) { panic("mockey") }
func (*lifetimeLog) OpenLogReader(context.Context, *wp.LogMessageId, string) (wp.LogReader, error) {
	panic("mockey")
}
func (*lifetimeLog) Close(context.Context) error { panic("mockey") }

type lifetimeReader struct{ wp.LogReader }

func (*lifetimeReader) ReadNext(context.Context) (*wp.LogMessage, error) { panic("mockey") }
func (*lifetimeReader) Close(context.Context) error                      { panic("mockey") }

func newLifetimeWAL() *walImpl {
	return &walImpl{
		WALHelper: helper.NewWALHelper(&walimpls.OpenOption{Channel: types.PChannelInfo{
			Name: "wp-lifetime", AccessMode: types.AccessModeRO,
		}}),
		l: &lifetimeLog{},
	}
}

func TestWriterOpenFailureClosesLog(t *testing.T) {
	for _, closeError := range []error{nil, context.DeadlineExceeded} {
		mockey.PatchConvey("each failed writer attempt closes its handle and preserves the original error", t, func() {
			mockey.Mock((*lifetimeClient).LogExists).Return(true, nil).Build()
			mockey.Mock((*lifetimeClient).OpenLog).To(func(*lifetimeClient, context.Context, string) (wp.LogHandle, error) {
				return &lifetimeLog{}, nil
			}).Build()
			ctx, cancel := context.WithCancel(context.Background())
			mockey.Mock((*lifetimeLog).OpenLogWriter).To(func(*lifetimeLog, context.Context) (wp.LogWriter, error) {
				cancel()
				return nil, context.Canceled
			}).Build()
			closed := 0
			mockey.Mock((*lifetimeLog).Close).To(func(_ *lifetimeLog, ctx context.Context) error {
				require.NoError(t, ctx.Err(), "cleanup must not inherit the failed attempt's cancellation")
				closed++
				return closeError
			}).Build()
			o := &openerImpl{c: &lifetimeClient{}}
			for i := 1; i <= 3; i++ {
				w, err := o.Open(ctx, &walimpls.OpenOption{Channel: types.PChannelInfo{
					Name: "wp-lifetime", AccessMode: types.AccessModeRW,
				}})
				require.Nil(t, w)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, i, closed)
			}
		})
	}
}

func TestReadOnlyLogWaitsForScanners(t *testing.T) {
	for _, count := range []int{0, 2} {
		mockey.PatchConvey("retiring an RO WAL releases its log only after all scanners close", t, func() {
			w := newLifetimeWAL()
			var closed, readersClosed atomic.Int32
			mockey.Mock((*lifetimeLog).Close).To(func(*lifetimeLog, context.Context) error {
				closed.Add(1)
				require.EqualValues(t, count, readersClosed.Load())
				return context.DeadlineExceeded // Failed cleanup is logged, never executed twice.
			}).Build()
			mockey.Mock((*lifetimeLog).OpenLogReader).To(func(*lifetimeLog, context.Context, *wp.LogMessageId, string) (wp.LogReader, error) {
				return &lifetimeReader{}, nil
			}).Build()
			mockey.Mock((*lifetimeReader).ReadNext).To(func(_ *lifetimeReader, ctx context.Context) (*wp.LogMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}).Build()
			mockey.Mock((*lifetimeReader).Close).To(func(*lifetimeReader, context.Context) error {
				readersClosed.Add(1)
				return context.Canceled
			}).Build()
			var scanners []walimpls.ScannerImpls
			opt := walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyAll()}
			for i := 0; i < count; i++ {
				s, err := w.Read(context.Background(), opt)
				require.NoError(t, err)
				scanners = append(scanners, s)
			}
			w.Close()
			w.Close()
			_, err := w.Read(context.Background(), opt)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			for _, s := range scanners {
				require.Zero(t, closed.Load())
				// Concurrent repeated Close must release the reader and its log reference once.
				var wg sync.WaitGroup
				for i := 0; i < 2; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); require.ErrorIs(t, s.Close(), context.Canceled) }()
				}
				wg.Wait()
			}
			require.EqualValues(t, 1, closed.Load())
		})
	}
}

func TestReadOnlyLogPinsInFlightOpen(t *testing.T) {
	for _, fail := range []bool{false, true} {
		mockey.PatchConvey("retiring RO during reader creation keeps the log alive until completion", t, func() {
			w := newLifetimeWAL()
			var closed atomic.Int32
			mockey.Mock((*lifetimeLog).Close).To(func(*lifetimeLog, context.Context) error {
				closed.Add(1)
				return nil
			}).Build()
			entered, resume := make(chan struct{}), make(chan struct{})
			mockey.Mock((*lifetimeLog).OpenLogReader).To(func(*lifetimeLog, context.Context, *wp.LogMessageId, string) (wp.LogReader, error) {
				close(entered)
				<-resume
				require.Zero(t, closed.Load())
				if fail {
					return nil, context.DeadlineExceeded
				}
				return &lifetimeReader{}, nil
			}).Build()
			mockey.Mock((*lifetimeReader).ReadNext).To(func(_ *lifetimeReader, ctx context.Context) (*wp.LogMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}).Build()
			mockey.Mock((*lifetimeReader).Close).Return(nil).Build()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s, err := w.Read(context.Background(), walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyAll()})
				if fail {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					return
				}
				require.NoError(t, err)
				require.NoError(t, s.Close())
			}()
			<-entered
			w.Close()
			require.Zero(t, closed.Load())
			close(resume)
			<-done
			require.EqualValues(t, 1, closed.Load())
		})
	}
}
