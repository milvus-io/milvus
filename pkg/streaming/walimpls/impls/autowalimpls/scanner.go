package autowalimpls

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (w *WAL) Read(ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	if _, latest := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_Latest); latest {
		return nil, merr.WrapErrParameterInvalidMsg("latest delivery policy is deprecated; use All, StartFrom or StartAfter")
	}
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
			var switchTimeTick uint64
			if message.IsWALSwitch(msg) {
				var err error
				start, err = message.WALSwitchStart(msg)
				if err != nil {
					return err
				}
				if start.WALName() == msg.WALName() {
					start = nil
				} else {
					// Consumers may overwrite message properties after handoff.
					switchTimeTick = msg.TimeTick()
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
			if switchTimeTick <= boundary {
				return merr.WrapErrDataIntegrityMsg("WAL scanner switch does not advance")
			}
			boundary = switchTimeTick
			next := s.readNext(w, opt, start)
			if next == nil {
				return nil // Scanner closed while establishing the next reader.
			}
			inner.Close()
			inner = next
		}
	}
}

// readNext retains the switch position until a target reader is established.
// Reopening the outer scanner here would replay its already delivered prefix.
// A nil result means the scanner was canceled.
func (s *scanner) readNext(w *WAL, opt walimpls.ReadOption, start message.MessageID) walimpls.ScannerImpls {
	ctx := s.Context()
	opt.DeliverPolicy = options.DeliverPolicyStartAfter(start)
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 10 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 0
	b.Reset()
	for ctx.Err() == nil {
		backend, err := w.backend(ctx, start.WALName(), false)
		if err == nil {
			var next walimpls.ScannerImpls
			next, err = backend.Read(ctx, opt)
			if err == nil {
				return next
			}
		}
		delay := b.NextBackOff()
		mlog.RatedWarn(ctx, 1, "WAL scanner switch failed, retrying", mlog.FieldPChannel(w.channel.Name), mlog.Stringer("targetStart", start), mlog.Duration("nextInterval", delay), mlog.Err(err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
	return nil
}
