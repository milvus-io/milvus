package adaptor

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var (
	_ recovery.RecoveryStreamBuilder = (*recoveryStreamBuilderImpl)(nil)
	_ recovery.RecoveryStream        = (*recoveryStreamImpl)(nil)
)

// newRecoveryStreamBuilder creates a new recovery stream builder.
func newRecoveryStreamBuilder(roWALImpls *roWALAdaptorImpl, basicWAL walimpls.WALImpls) *recoveryStreamBuilderImpl {
	return &recoveryStreamBuilderImpl{
		roWALAdaptorImpl: roWALImpls,
		basicWAL:         basicWAL,
	}
}

// recoveryStreamBuilerImpl is the implementation of RecoveryStreamBuilder.
type recoveryStreamBuilderImpl struct {
	*roWALAdaptorImpl
	basicWAL walimpls.WALImpls
}

// Build builds a recovery stream.
func (b *recoveryStreamBuilderImpl) Build(param recovery.BuildRecoveryStreamParam) recovery.RecoveryStream {
	scanner := newRecoveryScannerAdaptor(b.roWALImpls, param.StartCheckpoint, b.scanMetrics.NewScannerMetrics())
	recoveryStream := &recoveryStreamImpl{
		notifier:  syncutil.NewAsyncTaskNotifier[error](),
		param:     param,
		scanner:   scanner,
		ch:        make(chan message.ImmutableMessage),
		txnBuffer: nil,
	}
	go recoveryStream.execute()
	return recoveryStream
}

func (b *recoveryStreamBuilderImpl) RWWALImpls() walimpls.WALImpls {
	return b.basicWAL
}

// recoveryStreamImpl is the implementation of RecoveryStream.
type recoveryStreamImpl struct {
	notifier  *syncutil.AsyncTaskNotifier[error]
	scanner   *scannerAdaptorImpl
	param     recovery.BuildRecoveryStreamParam
	ch        chan message.ImmutableMessage
	txnBuffer *utility.TxnBuffer
}

// Chan returns the channel of the recovery stream.
func (r *recoveryStreamImpl) Chan() <-chan message.ImmutableMessage {
	return r.ch
}

// Error returns the error of the recovery stream.
func (r *recoveryStreamImpl) Error() error {
	return r.notifier.BlockAndGetResult()
}

// TxnBuffer returns the uncommitted txn buffer after recovery stream is done.
func (r *recoveryStreamImpl) TxnBuffer() *utility.TxnBuffer {
	if err := r.notifier.BlockAndGetResult(); err != nil {
		panic("TxnBuffer should only be called after recovery stream is done")
	}
	return r.txnBuffer
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
		if err == nil {
			// get the txn buffer after the consuming is done.
			r.txnBuffer = r.scanner.txnBuffer
		}
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
			if pendingMessage.TimeTick() == r.param.EndTimeTick {
				// reach the end of recovery stream, stop the consuming.
				return nil
			}
			pendingMessage = nil
		case msg, ok := <-upstream:
			if !ok {
				return r.scanner.Error()
			}
			pendingMessage = msg
		}
	}
}
