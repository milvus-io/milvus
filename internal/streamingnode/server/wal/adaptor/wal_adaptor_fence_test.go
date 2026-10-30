package adaptor

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	uberatomic "go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	lockinterceptor "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const fenceTestTimeout = 5 * time.Second

func TestReplicaOrFlushAppendStopsOnWALCancellation(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		t.Run(strconv.FormatBool(replicated), func(t *testing.T) {
			var persisted atomic.Int32
			var cancelWAL context.CancelFunc
			walImpls := newFirstTimeTickWALImpls(func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
				persisted.Add(1)
				cancelWAL() // Recovery will determine the durable outcome.
				<-ctx.Done()
				return nil, ctx.Err()
			})
			w := newFenceTestWAL(t, walImpls)
			cancelWAL = w.availableCancel
			var msg message.MutableMessage
			if replicated {
				msg = message.CreateTestEmptyInsertMesage(1, nil).WithReplicateHeader(&message.ReplicateHeader{
					ClusterID: "source", VChannel: "v1", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(0), TimeTick: 100,
				})
			} else {
				msg = message.NewFlushAllMessageBuilderV2().WithAllVChannel().WithUnreplicable().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).MustBuildMutable()
			}
			msg.WithTimeTick(100).WithLastConfirmedUseMessageID()
			_, err := w.Append(context.Background(), msg)
			require.ErrorIs(t, err, context.Canceled)
			require.Error(t, w.availableCtx.Err())
			_, err = w.Append(context.Background(), message.CreateTestEmptyInsertMesage(1, nil))
			require.Error(t, err)
			require.EqualValues(t, 1, persisted.Load(), "an uncertain append must be recovered before any further write")
		})
	}
}

func TestAppendBeforeAlterWALCompletesBeforeFence(t *testing.T) {
	insertStarted := make(chan struct{})
	releaseInsert := make(chan struct{})

	var nextID atomic.Int64
	var mu sync.Mutex
	var appendedTypes []message.MessageType
	walImpls := newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		if msg.MessageType() == message.MessageTypeInsert {
			close(insertStarted)
			<-releaseInsert
		}
		mu.Lock()
		appendedTypes = append(appendedTypes, msg.MessageType())
		mu.Unlock()
		return finishFenceTestAppend(ctx, nextID.Add(1)), nil
	})
	w := newFenceTestWAL(t, walImpls)

	insertErr := make(chan error, 1)
	go func() {
		_, err := w.Append(context.Background(), message.CreateTestEmptyInsertMesage(1, nil))
		insertErr <- err
	}()
	waitFenceTestSignal(t, insertStarted)

	alterErr := make(chan error, 1)
	go func() {
		_, err := w.Append(context.Background(), newFenceTestAlterWALMessage())
		alterErr <- err
	}()

	close(releaseInsert)
	require.NoError(t, waitFenceTestError(t, insertErr))
	require.NoError(t, waitFenceTestError(t, alterErr))
	assert.True(t, w.isFenced.Load())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []message.MessageType{
		message.MessageTypeInsert,
		message.MessageTypeAlterWAL,
	}, appendedTypes)
}

func TestAppendWaitingBehindAlterWALIsFenced(t *testing.T) {
	gate := &fenceTestGateInterceptor{
		insertEntered: make(chan struct{}),
		releaseInsert: make(chan struct{}),
		alterAppended: make(chan struct{}),
		releaseAlter:  make(chan struct{}),
	}

	var nextID atomic.Int64
	var mu sync.Mutex
	var appendedTypes []message.MessageType
	walImpls := newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		mu.Lock()
		appendedTypes = append(appendedTypes, msg.MessageType())
		mu.Unlock()
		return finishFenceTestAppend(ctx, nextID.Add(1)), nil
	})
	w := newFenceTestWAL(t, walImpls, gate)

	insertErr := make(chan error, 1)
	go func() {
		_, err := w.Append(context.Background(), message.CreateTestEmptyInsertMesage(1, nil))
		insertErr <- err
	}()
	waitFenceTestSignal(t, gate.insertEntered)

	alterErr := make(chan error, 1)
	go func() {
		_, err := w.Append(context.Background(), newFenceTestAlterWALMessage())
		alterErr <- err
	}()
	waitFenceTestSignal(t, gate.alterAppended)

	// The AlterWAL append has returned from the lock interceptor, but the outer
	// interceptor keeps walAdaptor.Append from reaching its post-chain logic.
	// The fence must already be visible when the waiting Insert acquires RLock.
	close(gate.releaseInsert)
	err := waitFenceTestError(t, insertErr)
	require.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsFenced())

	close(gate.releaseAlter)
	require.NoError(t, waitFenceTestError(t, alterErr))
	assert.True(t, w.isFenced.Load())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []message.MessageType{message.MessageTypeAlterWAL}, appendedTypes)
}

// The WAL detaches an append from its caller: client cancellation must not
// reach the interceptor chain, while the WAL becoming unavailable, which WAL
// close and fence both end in, must cancel it. Waits inside the chain, such as
// a write waiting for partial-update row locks, rely on both halves to end only
// with the WAL.
func TestAppendContextEndsWithWALNotClient(t *testing.T) {
	gate := &contextTestGateInterceptor{entered: make(chan struct{})}
	walImpls := newFirstTimeTickWALImpls(func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
		return finishFenceTestAppend(ctx, 1), nil
	})
	w := newFenceTestWAL(t, walImpls, gate)

	clientCtx, cancelClient := context.WithCancel(context.Background())
	appendErr := make(chan error, 1)
	go func() {
		_, err := w.Append(clientCtx, message.CreateTestEmptyInsertMesage(1, nil))
		appendErr <- err
	}()
	waitFenceTestSignal(t, gate.entered)
	cancelClient()
	select {
	case err := <-appendErr:
		t.Fatalf("client cancellation reached the interceptor chain: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Close and fence cancel availableCtx through forceCancelAfterGracefulTimeout.
	w.availableCancel()
	require.ErrorIs(t, waitFenceTestError(t, appendErr), context.Canceled)
}

type contextTestGateInterceptor struct {
	entered chan struct{}
}

func (i *contextTestGateInterceptor) DoAppend(
	ctx context.Context,
	msg message.MutableMessage,
	append interceptors.Append,
) (message.MessageID, error) {
	if msg.MessageType() != message.MessageTypeInsert {
		return append(ctx, msg)
	}
	close(i.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (i *contextTestGateInterceptor) Close() {}

type fenceTestGateInterceptor struct {
	insertEntered chan struct{}
	releaseInsert chan struct{}
	alterAppended chan struct{}
	releaseAlter  chan struct{}
}

func (i *fenceTestGateInterceptor) DoAppend(
	ctx context.Context,
	msg message.MutableMessage,
	append interceptors.Append,
) (message.MessageID, error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		close(i.insertEntered)
		<-i.releaseInsert
		return append(ctx, msg)
	case message.MessageTypeAlterWAL:
		messageID, err := append(ctx, msg)
		close(i.alterAppended)
		<-i.releaseAlter
		return messageID, err
	default:
		return append(ctx, msg)
	}
}

func (i *fenceTestGateInterceptor) Close() {}

func newFenceTestWAL(
	t *testing.T,
	walImpls walimpls.WALImpls,
	beforeLock ...interceptors.Interceptor,
) *walAdaptorImpl {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)

	channel := walImpls.Channel()
	txnManager := txn.NewTxnManager(channel, nil)
	param := &interceptors.InterceptorBuildParam{
		ChannelInfo: channel,
		TxnManager:  txnManager,
	}
	lockInterceptor := lockinterceptor.NewInterceptorBuilder().Build(param)
	allInterceptors := append(beforeLock, lockInterceptor)
	chainedInterceptor := interceptors.NewChainedInterceptor(allInterceptors...)

	availableCtx, availableCancel := context.WithCancel(context.Background())
	rateLimitComponent := rate.NewWALRateLimitComponent(channel)
	roWAL := &roWALAdaptorImpl{
		WALRateLimitComponent: rateLimitComponent,
		lifetime:              typeutil.NewLifetime(),
		availableCtx:          availableCtx,
		availableCancel:       availableCancel,
		roWALImpls:            walImpls,
	}
	roWAL.SetLogger(mlog.With())

	writeMetrics := metricsutil.NewWriteMetrics(channel, walImpls.WALName())
	writeMetrics.SetLogger(roWAL.Logger())
	w := &walAdaptorImpl{
		roWALAdaptorImpl: roWAL,
		rwWALImpls:       walImpls,
		interceptorBuildResult: interceptorBuildResult{
			Interceptor:       chainedInterceptor,
			GracefulCloseFunc: func() {},
		},
		writeMetrics:      writeMetrics,
		isFenced:          uberatomic.NewBool(false),
		appendRateCounter: utility.NewAverageRateCounter(10 * time.Second),
	}

	t.Cleanup(func() {
		availableCancel()
		chainedInterceptor.Close()
		writeMetrics.Close()
		rateLimitComponent.Close()
		require.NoError(t, txnManager.GracefulClose(context.Background()))
	})
	return w
}

func newFenceTestAlterWALMessage() message.MutableMessage {
	return message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
		WithBody(&message.AlterWALMessageBody{}).
		WithAllVChannel().
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmed(walimplstest.NewTestMessageID(0))
}

func finishFenceTestAppend(ctx context.Context, id int64) message.MessageID {
	messageID := walimplstest.NewTestMessageID(id)
	utility.ReplaceAppendResultTimeTick(ctx, uint64(id))
	utility.ReplaceAppendResultLastConfirmedMessageID(ctx, messageID)
	return messageID
}

func waitFenceTestSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(fenceTestTimeout):
		t.Fatal("timed out waiting for test signal")
	}
}

func waitFenceTestError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(fenceTestTimeout):
		t.Fatal("timed out waiting for append result")
		return nil
	}
}
