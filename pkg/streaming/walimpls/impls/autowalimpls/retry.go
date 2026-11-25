package autowalimpls

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// retry receives the WAL-owned context. Backend attempt cancellation alone does
// not end an accepted write; fencing or cancellation of this context does.
func retry(ctx context.Context, channel string, msg message.MutableMessage, attempt func() error) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 10 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 0
	b.Reset()
	for i := 0; ; i++ {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		err := attempt()
		if err == nil || errors.Is(err, walimpls.ErrFenced) {
			return err
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		metrics.WALImplsAppendRetryTotal.WithLabelValues(paramtable.GetStringNodeID(), channel).Inc()
		delay := b.NextBackOff()
		mlog.RatedWarn(ctx, 1, "WAL physical operation failed, retrying", mlog.FieldPChannel(channel), mlog.FieldMessage(msg), mlog.Int("retry", i), mlog.Duration("nextInterval", delay), mlog.Err(err))
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(delay):
		}
	}
}

// The backend and message stay fixed across attempts, including chunk identity
// and TimeTick. A lost acknowledgement may leave duplicate physical records.
func appendWithRetry(ctx context.Context, backend walimpls.WALImpls, msg message.MutableMessage) (message.MessageID, error) {
	var id message.MessageID
	err := retry(ctx, backend.Channel().Name, msg, func() error {
		var err error
		id, err = backend.Append(ctx, msg)
		return err
	})
	if err != nil {
		return nil, err
	}
	return id, nil
}
