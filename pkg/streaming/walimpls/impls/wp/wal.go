package wp

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/zilliztech/woodpecker/common/werr"
	wp "github.com/zilliztech/woodpecker/woodpecker/log"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	p wp.LogWriter
	l wp.LogHandle

	// RO handles can be retired while their scanners are still consuming.
	mu      sync.Mutex
	closed  bool
	readers int
}

func (w *walImpl) WALName() message.WALName {
	return message.WALNameWoodpecker
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("write on a wal that is not in read-write mode")
	}
	pb := msg.IntoMessageProto()
	r := w.p.Write(ctx,
		&wp.WriteMessage{
			Payload:    pb.Payload,
			Properties: pb.Properties,
		},
	)
	if r.Err != nil {
		if werr.ErrLogWriterLockLost.Is(r.Err) {
			w.Log().RatedWarn(ctx, rate.Limit(1), "wp writer fenced", mlog.Err(r.Err))
			return nil, errors.Mark(r.Err, walimpls.ErrFenced)
		}
		w.Log().RatedWarn(ctx, rate.Limit(1), "write message to woodpecker failed", mlog.Err(r.Err))
		return nil, r.Err
	}
	return wpID{r.LogMessageId}, nil
}

func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	if _, latest := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_Latest); latest {
		return nil, merr.WrapErrParameterInvalidMsg("latest delivery policy is deprecated; use All, StartFrom or StartAfter")
	}
	from := wp.EarliestLogMessageID()

	switch t := opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		from = wp.EarliestLogMessageID()
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(t.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		from.SegmentId = id.logMsgId.SegmentId
		from.EntryId = id.logMsgId.EntryId
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		from.SegmentId = id.logMsgId.SegmentId
		from.EntryId = id.logMsgId.EntryId + 1
	}

	var release func()
	if w.p == nil {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return nil, merr.WrapErrServiceUnavailableMsg("woodpecker read-only WAL closed")
		}
		w.readers++
		w.mu.Unlock()
		release = w.releaseReader
	}
	reader, err := w.l.OpenLogReader(ctx, &from, opt.Name)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	return newScanner(opt.Name, reader, release), nil
}

func (w *walImpl) Truncate(ctx context.Context, id message.MessageID) error {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("truncate on a wal that is not in read-write mode")
	}
	wpId := id.(wpID)
	return w.l.Truncate(ctx, wpId.logMsgId)
}

func (w *walImpl) Close() {
	if w.p == nil {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		w.closed = true
		closeLog := w.readers == 0
		w.mu.Unlock()
		if closeLog {
			w.closeLog()
		}
		return
	}
	closeWriterErr := w.p.Close(context.Background())
	if closeWriterErr != nil {
		w.Log().Warn(context.TODO(), "close woodpecker writer err", mlog.Err(closeWriterErr))
	}
}

func (w *walImpl) releaseReader() {
	w.mu.Lock()
	w.readers--
	closeLog := w.closed && w.readers == 0
	w.mu.Unlock()
	if closeLog {
		w.closeLog()
	}
}

func (w *walImpl) closeLog() {
	if err := w.l.Close(context.Background()); err != nil {
		w.Log().Warn(context.TODO(), "close woodpecker log handle err", mlog.Err(err))
	}
}
