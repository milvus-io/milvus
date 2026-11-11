package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/autowalimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		initialWAL:       roWALImpls.roWALImpls.(walimpls.WALImpls),
		writers:          map[message.WALName]walimpls.WALImpls{roWALImpls.WALName(): roWALImpls.roWALImpls.(walimpls.WALImpls)},
	}
}

// recoveryStreamBuilerImpl is the implementation of RecoveryStreamBuilder.
type recoveryStreamBuilderImpl struct {
	*roWALAdaptorImpl
	basicWAL         walimpls.WALImpls
	writeAheadBuffer wab.ROWriteAheadBuffer
	liveReady        <-chan struct{}
	initialWAL       walimpls.WALImpls
	openBackend      autowalimpls.OpenBackend
	writers          map[message.WALName]walimpls.WALImpls
	transferred      bool
}

// Build builds a recovery stream.
func (b *recoveryStreamBuilderImpl) Build(param recovery.BuildRecoveryStreamParam) recovery.RecoveryStream {
	policy := options.DeliverPolicyStartFrom(param.StartCheckpoint)
	if param.StartAfterCheckpoint {
		policy = options.DeliverPolicyStartAfter(param.StartCheckpoint)
	}
	scanner := newScannerAdaptor(
		"recovery",
		b.basicWAL,
		wal.ReadOption{
			DeliverPolicy:          policy,
			IgnorePauseConsumption: true,
		},
		b.scanMetrics.NewScannerMetrics(),
		func() {},
		scannerConfig{
			writeAheadBuffer: b.writeAheadBuffer,
			startupBarrier: &scannerStartupBarrier{
				message: param.RecoveryBarrier, resume: b.liveReady, stopAtWALSwitch: param.StopAtWALSwitch,
				liveSource: func() (walimpls.ROWALImpls, wab.ROWriteAheadBuffer) { return b.basicWAL, b.writeAheadBuffer },
			},
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

// SwitchWAL is called only by RecoveryStorage after observing an AlterWAL.
func (b *recoveryStreamBuilderImpl) SwitchWAL(ctx context.Context, start message.MessageID) (message.ImmutableMessage, error) {
	target := b.writers[start.WALName()]
	if target == nil {
		if b.openBackend == nil {
			return nil, merr.WrapErrServiceInternalMsg("missing recovery WAL opener")
		}
		var err error
		target, err = b.openBackend(ctx, start.WALName(), b.initialWAL.Channel().AccessMode)
		if err != nil {
			return nil, err
		}
		b.writers[start.WALName()] = target
	}
	barrier, err := sendFirstTimeTick(ctx, target, start)
	if err != nil {
		return nil, err
	}
	b.basicWAL = target
	return barrier, nil
}

// finishRecovery transfers owned writers only after the data replay succeeds.
func (b *recoveryStreamBuilderImpl) finishRecovery() walimpls.WALImpls {
	if b.openBackend != nil {
		writers := make([]walimpls.WALImpls, 0, len(b.writers))
		for _, writer := range b.writers {
			writers = append(writers, writer)
		}
		b.basicWAL = autowalimpls.New(b.basicWAL, b.openBackend, writers...)
	}
	b.transferred = true
	return b.basicWAL
}

func (b *recoveryStreamBuilderImpl) closeUntransferredWriters() {
	if b.transferred {
		return
	}
	// The opener's resource guard owns the original physical handle.
	for _, writer := range b.writers {
		if writer != b.initialWAL {
			writer.Close()
		}
	}
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
	return r.notifier.BlockAndGetResult()
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
