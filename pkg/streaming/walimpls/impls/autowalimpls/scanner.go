package autowalimpls

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (w *WAL) Read(ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	name := w.WALName()
	var exclude message.MessageID
	switch p := opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := message.UnmarshalMessageID(p.StartFrom)
		if err != nil {
			return nil, err
		}
		name = id.WALName()
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := message.UnmarshalMessageID(p.StartAfter)
		if err != nil {
			return nil, err
		}
		name = id.WALName()
		// Inspect an excluded AlterWAL as routing metadata before filtering it.
		exclude = id
		opt.DeliverPolicy = options.DeliverPolicyStartFrom(id)
	}
	b, err := w.backend(ctx, name, false)
	if err != nil {
		return nil, err
	}
	inner, err := b.Read(ctx, opt)
	if err != nil {
		return nil, err
	}
	s := &scanner{ScannerHelper: helper.NewScannerHelper(opt.Name), ch: make(chan message.ImmutableMessage, 1)}
	go func() { defer close(s.ch); s.Finish(s.run(w, inner, opt, exclude)) }()
	return s, nil
}

type scanner struct {
	*helper.ScannerHelper
	ch chan message.ImmutableMessage
}

func (s *scanner) Chan() <-chan message.ImmutableMessage { return s.ch }

func (s *scanner) run(w *WAL, inner walimpls.ScannerImpls, opt walimpls.ReadOption, exclude message.MessageID) error {
	defer func() { inner.Close() }()
	var boundary uint64
	for {
		select {
		case <-s.Context().Done():
			return nil
		case msg, ok := <-inner.Chan():
			if !ok {
				return inner.Error()
			}
			var start message.MessageID
			if message.IsWALSwitch(msg) {
				var err error
				start, err = message.WALSwitchStart(msg)
				if err != nil {
					return err
				}
				if start.WALName() == msg.WALName() {
					start = nil
				}
			}
			skip := exclude != nil && msg.MessageID().EQ(exclude)
			exclude = nil
			if !skip {
				select {
				case <-s.Context().Done():
					return nil
				case s.ch <- msg:
				}
			}
			if start == nil {
				continue
			}
			if msg.TimeTick() <= boundary {
				return merr.WrapErrDataIntegrityMsg("WAL scanner switch does not advance")
			}
			boundary = msg.TimeTick()
			b, err := w.backend(s.Context(), start.WALName(), false)
			if err != nil {
				return err
			}
			opt.DeliverPolicy = options.DeliverPolicyStartAfter(start)
			next, err := b.Read(s.Context(), opt)
			if err != nil {
				return err
			}
			inner.Close()
			inner = next
		}
	}
}
