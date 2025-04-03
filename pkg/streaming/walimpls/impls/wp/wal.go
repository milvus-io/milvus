package wp

import (
	"context"

	wp "github.com/zilliztech/woodpecker/woodpecker/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	p wp.LogWriter
	l wp.LogHandle
}

func (w *walImpl) WALName() string {
	return walName
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	r := w.p.Write(ctx,
		&wp.WriterMessage{
			Payload:    msg.Payload(),
			Properties: msg.Properties().ToRawMap(),
		},
	)
	if r.Err != nil {
		w.Log().RatedWarn(1, "write message to woodpecker failed", zap.Error(r.Err))
		return nil, r.Err
	}
	return wpID{r.LogMessageId}, nil
}

func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	from := wp.LatestLogMessageID()

	switch t := opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		from = wp.EarliestLogMessageID()
	case *streamingpb.DeliverPolicy_Latest:
		from = wp.LatestLogMessageID()
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

	reader, err := w.l.OpenLogReader(ctx, &from, opt.Name)
	if err != nil {
		return nil, err
	}
	return newScanner(opt.Name, reader), nil
}

func (w *walImpl) Close() {
	closeWriterErr := w.p.Close(context.Background())
	if closeWriterErr != nil {
		w.Log().Warn("close woodpecker writer err", zap.Error(closeWriterErr))
	}
}
