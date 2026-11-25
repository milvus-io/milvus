package autowalimpls

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type scannerOpenFailureLog struct{ *physicalLog }

func (b *scannerOpenFailureLog) Read(context.Context, walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	panic("mockey")
}

func prepareScannerSwitch(t *testing.T) (*physicalLog, *physicalLog, message.MessageID, message.MessageID) {
	t.Helper()
	p, w, open := installBackends(t)
	writer := New(p, open)
	t.Cleanup(writer.Close)
	ctx := context.Background()
	first, err := writer.Append(ctx, ordinary(10, nil))
	require.NoError(t, err)
	a := alter(20, w.name, first)
	_, err = writer.Append(ctx, a)
	require.NoError(t, err)
	start, err := message.WALSwitchStart(a)
	require.NoError(t, err)
	_, err = writer.Append(ctx, ordinary(30, start))
	require.NoError(t, err)
	return p, w, first, start
}

func TestScannerRetriesSwitchWithoutReplayingPrefix(t *testing.T) {
	mockey.PatchConvey("target open and read failures retain the logical scanner and anchor", t, func() {
		p, w, first, start := prepareScannerSwitch(t)
		var opens, reads atomic.Int32
		mockey.Mock((*scannerOpenFailureLog).Read).To(func(b *scannerOpenFailureLog, ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
			require.Equal(t, "switch-retry", opt.Name)
			require.Equal(t, options.DeliverPolicyStartAfter(start), opt.DeliverPolicy)
			if reads.Add(1) <= 2 {
				// An attempt timeout must not cancel the logical scanner.
				return nil, context.DeadlineExceeded
			}
			return b.physicalLog.Read(ctx, opt)
		}).Build()
		auto := New(p, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			require.Equal(t, w.name, name)
			require.Equal(t, types.AccessModeRO, mode)
			if opens.Add(1) <= 2 {
				return nil, merr.WrapErrServiceUnavailableMsg("target temporarily unavailable")
			}
			return &scannerOpenFailureLog{w}, nil
		})
		defer auto.Close()
		s, err := auto.Read(context.Background(), walimpls.ReadOption{Name: "switch-retry", DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		for _, tick := range []uint64{10, 20, 30} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
		require.EqualValues(t, 3, opens.Load(), "successful backend is reused across reader creation retries")
		require.EqualValues(t, 3, reads.Load())
		_, err = w.Append(context.Background(), ordinary(40, start))
		require.NoError(t, err)
		require.Equal(t, uint64(40), receive(t, s).TimeTick())
		require.NoError(t, s.Close())
		_, ok := <-s.Chan()
		require.False(t, ok, "no replayed prefix, boundary or anchor may remain queued")
	})
}

func TestScannerTransfersMessageOwnership(t *testing.T) {
	mockey.PatchConvey("consumers can overwrite trace context immediately after receiving a switch", t, func() {
		p, w, first, _ := prepareScannerSwitch(t)
		auto := New(p, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			require.Equal(t, w.name, name)
			require.Equal(t, types.AccessModeRO, mode)
			return &readOnlyLog{w}, nil
		})
		defer auto.Close()
		ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1},
		}))
		// Repeated handoffs exercise both goroutines under the race detector.
		for i := 0; i < 32; i++ {
			s, err := auto.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
			require.NoError(t, err)
			for _, tick := range []uint64{10, 20, 30} {
				msg := receive(t, s)
				message.OverwriteTraceContext(ctx, msg)
				require.Equal(t, tick, msg.TimeTick())
			}
			require.NoError(t, s.Close())
		}
	})
}

func TestScannerRejectsNonAdvancingSwitch(t *testing.T) {
	mockey.PatchConvey("cached switch TimeTick still enforces increasing boundaries", t, func() {
		p, w, first, start := prepareScannerSwitch(t)
		invalid := alter(20, p.name, start)
		message.SetWALSwitchStart(invalid, first)
		_, err := w.Append(context.Background(), invalid)
		require.NoError(t, err)
		auto := New(p, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
			return &readOnlyLog{w}, nil
		})
		defer auto.Close()
		s, err := auto.Read(context.Background(), walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		for _, tick := range []uint64{10, 20, 30, 20} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
		select {
		case <-s.Done():
			require.ErrorContains(t, s.Error(), "WAL scanner switch does not advance")
		case <-time.After(time.Second):
			t.Fatal("scanner did not reject the non-advancing switch")
		}
	})
}

func TestScannerSwitchRetryCancellation(t *testing.T) {
	for _, stage := range []string{"open", "read"} {
		for _, inFlight := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: "/backoff", true: "/in-flight"}[inFlight], func(t *testing.T) {
				mockey.PatchConvey("closing the logical scanner cancels handoff retry", t, func() {
					p, w, first, _ := prepareScannerSwitch(t)
					attempted := make(chan struct{})
					var once sync.Once
					fail := func(ctx context.Context) error {
						once.Do(func() { close(attempted) })
						if inFlight {
							<-ctx.Done()
							return ctx.Err()
						}
						return merr.WrapErrServiceUnavailableMsg("target temporarily unavailable")
					}
					mockey.Mock((*scannerOpenFailureLog).Read).To(func(_ *scannerOpenFailureLog, ctx context.Context, _ walimpls.ReadOption) (walimpls.ScannerImpls, error) {
						return nil, fail(ctx)
					}).Build()
					auto := New(p, func(ctx context.Context, _ message.WALName, _ types.AccessMode) (walimpls.WALImpls, error) {
						if stage == "open" {
							return nil, fail(ctx)
						}
						return &scannerOpenFailureLog{w}, nil
					})
					defer auto.Close()
					s, err := auto.Read(context.Background(), walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
					require.NoError(t, err)
					defer s.Close()
					for _, tick := range []uint64{10, 20} {
						require.Equal(t, tick, receive(t, s).TimeTick())
					}
					select {
					case <-attempted:
					case <-time.After(time.Second):
						t.Fatal("switch was not attempted")
					}
					s.(*scanner).Cancel()
					select {
					case <-s.Done():
					case <-time.After(time.Second):
						t.Fatal("scanner cancellation did not end handoff")
					}
					require.NoError(t, s.Error())
					_, ok := <-s.Chan()
					require.False(t, ok)
				})
			})
		}
	}
}

func TestScannerSwitchAlreadyCanceled(t *testing.T) {
	s := &scanner{ScannerHelper: helper.NewScannerHelper("closed")}
	s.Cancel()
	start := walimplstest.NewTestMessageID(0)
	require.Nil(t, s.readNext(nil, walimpls.ReadOption{}, start))
}
