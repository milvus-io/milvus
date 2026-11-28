package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// Writer recovery borrows the read replica's assembled stream. Only the writer
// consumer stops at its startup barrier; the replica scanner keeps running.
type replicaRecoveryBuilder struct {
	walimpls.WALImpls
	cursor  *replica.RecoveryCursor
	ready   <-chan struct{}
	onFatal func(error)
}

func (b *replicaRecoveryBuilder) RWWALImpls() walimpls.WALImpls { return b.WALImpls }
func (b *replicaRecoveryBuilder) Build(p recovery.BuildRecoveryStreamParam) recovery.RecoveryStream {
	s := &replicaRecoveryStream{ch: make(chan message.ImmutableMessage), notifier: syncutil.NewAsyncTaskNotifier[error]()}
	go func() {
		err := s.consume(b, p.RecoveryBarrier)
		if err != nil && s.notifier.Context().Err() == nil {
			b.onFatal(err)
		}
		b.cursor.Close()
		close(s.ch)
		s.notifier.Finish(err)
	}()
	return s
}

type replicaRecoveryStream struct {
	ch           chan message.ImmutableMessage
	notifier     *syncutil.AsyncTaskNotifier[error]
	transactions *utility.TxnBuffer
}

func (s *replicaRecoveryStream) consume(b *replicaRecoveryBuilder, barrier message.ImmutableMessage) error {
	ctx := s.notifier.Context()
	for {
		batch, err := b.cursor.Next(ctx)
		if err != nil {
			return err
		}
		for _, msg := range batch {
			cut := msg.MessageType() == message.MessageTypeRecoveryBarrier && msg.TimeTick() == barrier.TimeTick() && msg.WALName() == barrier.WALName() && msg.MessageID().EQ(barrier.MessageID())
			if cut {
				var ok bool
				if s.transactions, ok = b.cursor.TakeTransactions(barrier); !ok {
					return merr.WrapErrServiceUnavailable("replica scanner lost writer startup barrier")
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.ch <- msg:
			}
			if cut {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-b.ready:
				}
			}
		}
	}
}
func (s *replicaRecoveryStream) Chan() <-chan message.ImmutableMessage { return s.ch }
func (s *replicaRecoveryStream) TxnBuffer() *utility.TxnBuffer         { return s.transactions }
func (s *replicaRecoveryStream) Error() error                          { return s.notifier.BlockAndGetResult() }
func (s *replicaRecoveryStream) Close() error                          { s.notifier.Cancel(); return s.Error() }

func (w *roWALAdaptorImpl) WaitThrough(ctx context.Context, ts uint64) error {
	return w.replicaBuffer.WaitThrough(ctx, ts)
}

// prepareReplicaWriter uses the live reader as the witness for physical writer
// selection. It retains the cursor's messages for ordinary bounded recovery.
func prepareReplicaWriter(ctx context.Context, physical *recoveryStreamBuilderImpl, cursor *replica.RecoveryCursor, buffer *replica.Buffer, checkpoint message.MessageID) (message.ImmutableMessage, error) {
	tick, start := buffer.WriterPosition()
	var probe message.ImmutableMessage
	var err error
	if start == nil {
		probe, err = sendFirstTimeTick(ctx, physical.RWWALImpls(), checkpoint)
	}
	for {
		if start != nil {
			probe, err = physical.SwitchWAL(ctx, start)
		}
		if err != nil {
			return nil, err
		}
		tick, start, err = cursor.WaitWriterBarrier(ctx, tick, probe)
		if err != nil {
			return nil, err
		}
		if start == nil {
			return probe, nil
		}
	}
}
