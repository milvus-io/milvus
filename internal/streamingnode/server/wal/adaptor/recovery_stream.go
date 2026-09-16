package adaptor

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var (
	_ recovery.RecoveryStreamBuilder = (*recoveryStreamBuilderImpl)(nil)
	_ recovery.RecoveryStream        = (*recoveryStreamImpl)(nil)
)

// newRecoveryStreamBuilder creates a new recovery stream builder.
func newRecoveryStreamBuilder(roWALImpls *roWALAdaptorImpl, buffer wab.ROWriteAheadBuffer, liveReady <-chan struct{}) *recoveryStreamBuilderImpl {
	return &recoveryStreamBuilderImpl{
		roWALAdaptorImpl: roWALImpls,
		basicWAL:         roWALImpls.roWALImpls.(walimpls.WALImpls),
		writeAheadBuffer: buffer,
		liveReady:        liveReady,
	}
}

// recoveryStreamBuilerImpl is the implementation of RecoveryStreamBuilder.
type recoveryStreamBuilderImpl struct {
	*roWALAdaptorImpl
	basicWAL         walimpls.WALImpls
	writeAheadBuffer wab.ROWriteAheadBuffer
	liveReady        <-chan struct{}
}

// Build builds a recovery stream.
func (b *recoveryStreamBuilderImpl) Build(param recovery.BuildRecoveryStreamParam) recovery.RecoveryStream {
	scanner := newScannerAdaptor(
		"recovery",
		b.roWALImpls,
		wal.ReadOption{
			DeliverPolicy:          options.DeliverPolicyStartFrom(param.StartCheckpoint),
			IgnorePauseConsumption: true,
		},
		b.scanMetrics.NewScannerMetrics(),
		func() {},
		scannerConfig{
			writeAheadBuffer: b.writeAheadBuffer,
			startupBarrier:   &scannerStartupBarrier{message: param.RecoveryBarrier, resume: b.liveReady},
		},
	)
	recoveryStream := &recoveryStreamImpl{
		notifier: syncutil.NewAsyncTaskNotifier[error](),
		scanner:  scanner,
		ch:       make(chan message.ImmutableMessage),
		onFatal:  b.markUnavailable,
	}
	go recoveryStream.execute()
	return recoveryStream
}

func (b *recoveryStreamBuilderImpl) RWWALImpls() walimpls.WALImpls {
	return b.basicWAL
}

// recoveryStreamImpl is the implementation of RecoveryStream.
type recoveryStreamImpl struct {
	notifier *syncutil.AsyncTaskNotifier[error]
	scanner  *scannerAdaptorImpl
	ch       chan message.ImmutableMessage
	onFatal  func(error)
}

// Chan returns the channel of the recovery stream.
func (r *recoveryStreamImpl) Chan() <-chan message.ImmutableMessage {
	return r.ch
}

// Error returns the error of the recovery stream.
func (r *recoveryStreamImpl) Error() error {
	return r.notifier.BlockAndGetResult()
}

// TxnBuffer returns the independent snapshot captured before the startup
// barrier was delivered. The scanner retains its own live buffer.
func (r *recoveryStreamImpl) TxnBuffer() *utility.TxnBuffer {
	return r.scanner.startupTxnBuffer
}

// Close closes the recovery stream.
func (r *recoveryStreamImpl) Close() error {
	r.notifier.Cancel()
	err := r.notifier.BlockAndGetResult()
	return err
}

// execute starts the recovery stream.
func (r *recoveryStreamImpl) execute() (err error) {
	defer func() {
		close(r.ch)
		r.scanner.Close()
		r.notifier.Finish(err)
	}()

	var pendingMessage message.ImmutableMessage
	var upstream <-chan message.ImmutableMessage
	var downstream chan<- message.ImmutableMessage

	for {
		if pendingMessage != nil {
			// if there is a pending message, we need to send it to the downstream.
			upstream = nil
			downstream = r.ch
		} else {
			// if there is no pending message, we need to read from the upstream.
			upstream = r.scanner.Chan()
			downstream = nil
		}

		select {
		case <-r.notifier.Context().Done():
			// canceled.
			return r.notifier.Context().Err()
		case downstream <- pendingMessage:
			pendingMessage = nil
		case msg, ok := <-upstream:
			if !ok {
				err := r.scanner.Error()
				if err != nil && r.notifier.Context().Err() == nil {
					r.onFatal(err)
				}
				return err
			}
			pendingMessage = msg
		}
	}
}
