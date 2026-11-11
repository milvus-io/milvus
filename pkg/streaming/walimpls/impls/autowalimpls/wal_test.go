package autowalimpls

import (
	"context"
	"sync"
	"testing"
	"time"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type physicalLog struct {
	mu              sync.Mutex
	name            message.WALName
	messages        []message.ImmutableMessage
	changed         chan struct{}
	failAfterAppend bool
	closed          bool
	truncated       message.MessageID
}

func (b *physicalLog) WALName() message.WALName { return b.name }
func (b *physicalLog) Channel() types.PChannelInfo {
	return types.PChannelInfo{Name: "auto-test", Term: 1, AccessMode: types.AccessModeRW}
}
func (b *physicalLog) Close() { b.mu.Lock(); b.closed = true; b.mu.Unlock() }
func (b *physicalLog) Append(context.Context, message.MutableMessage) (message.MessageID, error) {
	panic("mockey")
}

func (b *physicalLog) Read(context.Context, walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	panic("mockey")
}

func (b *physicalLog) Truncate(_ context.Context, id message.MessageID) error {
	b.truncated = id
	return nil
}

type physicalScanner struct {
	*helper.ScannerHelper
	ch chan message.ImmutableMessage
}

func (s *physicalScanner) Chan() <-chan message.ImmutableMessage { return s.ch }

func installBackends(t *testing.T) (*physicalLog, *physicalLog, OpenBackend) {
	p := &physicalLog{name: message.WALNamePulsar, changed: make(chan struct{})}
	w := &physicalLog{name: message.WALNameWoodpecker, changed: make(chan struct{})}
	mockey.Mock((*physicalLog).Append).To(func(b *physicalLog, _ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		n := int64(len(b.messages))
		var id message.MessageID
		if b.name == message.WALNamePulsar {
			id = pulsar.NewPulsarID(pulsarclient.NewMessageID(1, n, 0, 0))
		} else {
			id = wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: n})
		}
		// Persist a detached property map; subsequent caller mutation cannot alter history.
		properties := make(map[string]string)
		for k, v := range msg.Properties().ToRawMap() {
			properties[k] = v
		}
		b.messages = append(b.messages, message.NewImmutableMesasge(id, append([]byte(nil), msg.Payload()...), properties))
		close(b.changed)
		b.changed = make(chan struct{})
		if b.failAfterAppend {
			b.failAfterAppend = false
			return nil, merr.WrapErrServiceUnavailableMsg("lost append response")
		}
		return id, nil
	}).Build()
	mockey.Mock((*physicalLog).Read).To(func(b *physicalLog, _ context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
		b.mu.Lock()
		offset := 0
		switch policy := opt.DeliverPolicy.GetPolicy().(type) {
		case *streamingpb.DeliverPolicy_Latest:
			offset = len(b.messages)
		case *streamingpb.DeliverPolicy_StartFrom:
			id := message.MustUnmarshalMessageID(policy.StartFrom)
			for offset < len(b.messages) && b.messages[offset].MessageID().LT(id) {
				offset++
			}
		case *streamingpb.DeliverPolicy_StartAfter:
			id := message.MustUnmarshalMessageID(policy.StartAfter)
			for offset < len(b.messages) && b.messages[offset].MessageID().LTE(id) {
				offset++
			}
		}
		b.mu.Unlock()
		s := &physicalScanner{ScannerHelper: helper.NewScannerHelper(opt.Name), ch: make(chan message.ImmutableMessage)}
		go func() {
			defer close(s.ch)
			defer s.Finish(nil)
			for {
				b.mu.Lock()
				if offset < len(b.messages) {
					msg := b.messages[offset]
					offset++
					b.mu.Unlock()
					select {
					case <-s.Context().Done():
						return
					case s.ch <- msg:
					}
				} else {
					changed := b.changed
					b.mu.Unlock()
					select {
					case <-s.Context().Done():
						return
					case <-changed:
					}
				}
			}
		}()
		return s, nil
	}).Build()
	return p, w, func(_ context.Context, name message.WALName, _ types.AccessMode) (walimpls.WALImpls, error) {
		if name == p.name {
			return p, nil
		}
		if name == w.name {
			return w, nil
		}
		return nil, merr.WrapErrServiceUnavailableMsg("backend unavailable")
	}
}

func ordinary(tick uint64, last message.MessageID) message.MutableMessage {
	m := message.NewFlushAllMessageBuilderV2().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(tick)
	if last == nil {
		return m.WithLastConfirmedUseMessageID()
	}
	return m.WithLastConfirmed(last)
}

func alter(tick uint64, target message.WALName, last message.MessageID) message.MutableMessage {
	return message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName(target)}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(tick).WithLastConfirmed(last)
}

func receive(t *testing.T, s walimpls.ScannerImpls) message.ImmutableMessage {
	t.Helper()
	select {
	case msg, ok := <-s.Chan():
		require.True(t, ok)
		return msg
	case <-time.After(3 * time.Second):
		t.Fatal("scanner stalled")
		return nil
	}
}

func TestLinkedWALSwitchRoundTrip(t *testing.T) {
	mockey.PatchConvey("linked native positions survive round trip and checkpoint lag", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		auto := New(p, open)
		first, err := auto.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		s, err := auto.Read(ctx, walimpls.ReadOption{Name: "slow", DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		a := alter(20, w.name, first)
		boundary, err := auto.Append(ctx, a)
		require.NoError(t, err)
		start, err := message.WALSwitchStart(a)
		require.NoError(t, err)
		next, err := auto.Append(ctx, ordinary(30, start))
		require.NoError(t, err)
		require.Equal(t, w.name, next.WALName())
		require.Equal(t, p.name, boundary.WALName())
		// Lost wrapper response: retry returns the original old-end ID.
		retryID, err := auto.Append(ctx, a)
		require.NoError(t, err)
		require.True(t, retryID.EQ(boundary))
		require.Len(t, p.messages, 2)
		for _, tick := range []uint64{10, 20, 30} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
		after, err := auto.Read(ctx, walimpls.ReadOption{Name: "after-boundary", DeliverPolicy: options.DeliverPolicyStartAfter(boundary)})
		require.NoError(t, err)
		require.Equal(t, uint64(30), receive(t, after).TimeTick())
		after.Close()
		back := alter(40, p.name, next)
		_, err = auto.Append(ctx, back)
		require.NoError(t, err)
		backStart, err := message.WALSwitchStart(back)
		require.NoError(t, err)
		last, err := auto.Append(ctx, ordinary(50, backStart))
		require.NoError(t, err)
		require.Equal(t, p.name, last.WALName())
		for _, tick := range []uint64{40, 50} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
		require.NoError(t, auto.Truncate(ctx, next))
		require.True(t, w.truncated.EQ(next))
		require.NoError(t, auto.Truncate(ctx, first))
		require.True(t, p.truncated.EQ(first))
	})
}

func TestLinkedWALCrashAfterBoundary(t *testing.T) {
	mockey.PatchConvey("durable old marker is authoritative even before writer swap", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		auto := New(p, open)
		first, err := auto.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		p.failAfterAppend = true
		a := alter(20, w.name, first)
		_, err = auto.Append(ctx, a)
		require.Error(t, err)
		require.Equal(t, p.name, auto.WALName())
		floor, err := message.WALSwitchStart(a)
		require.NoError(t, err)
		// RecoveryStorage supplies the final writer and transfers all acquired handles.
		recovered := New(w, open, p, w)
		require.Equal(t, w.name, recovered.WALName())
		require.Len(t, w.messages, 1, "empty target has only its durable anchor")
		_, err = recovered.Append(ctx, ordinary(30, floor))
		require.NoError(t, err)
		s, err := recovered.Read(ctx, walimpls.ReadOption{Name: "replay", DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		for _, tick := range []uint64{10, 20, 30} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
	})
}

func TestLinkedWALEmptyAndRedundantSwitch(t *testing.T) {
	mockey.PatchConvey("empty startup and same-backend barrier do not loop", t, func() {
		p, _, open := installBackends(t)
		ctx := context.Background()
		auto := New(p, open)
		first, err := auto.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		_, err = auto.Append(ctx, alter(20, p.name, first))
		require.NoError(t, err)
		require.Len(t, p.messages, 2)
		auto.Close()
		auto.Close()
		require.True(t, p.closed)
		_, err = auto.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyAll()})
		require.Error(t, err)
	})
}

func TestPreparedAnchorIsNotACommittedSwitch(t *testing.T) {
	mockey.PatchConvey("lost target append response leaves old writer authoritative", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		auto := New(p, open)
		first, err := auto.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		w.failAfterAppend = true
		a := alter(20, w.name, first)
		_, err = auto.Append(ctx, a)
		require.Error(t, err)
		require.Len(t, p.messages, 1)
		require.Equal(t, p.name, auto.WALName())
		_, err = auto.Append(ctx, a)
		require.NoError(t, err)
		require.Len(t, w.messages, 2, "retry may leave an unreferenced anchor")
		start, err := message.WALSwitchStart(a)
		require.NoError(t, err)
		_, err = auto.Append(ctx, ordinary(30, start))
		require.NoError(t, err)
		s, err := auto.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
		require.NoError(t, err)
		defer s.Close()
		for _, tick := range []uint64{10, 20, 30} {
			require.Equal(t, tick, receive(t, s).TimeTick())
		}
	})
}

func TestLinkedWALRejectsUnavailableAndInvalidLinks(t *testing.T) {
	mockey.PatchConvey("failed routing never permits old-prefix continuation", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		auto := New(p, open)
		require.Equal(t, p.Channel(), auto.Channel())
		first, err := auto.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		_, err = auto.Append(ctx, alter(20, message.WALNameKafka, first))
		require.Error(t, err)
		a := alter(20, w.name, first)
		typed := message.MustAsMutableAlterWALMessageV2(a)
		header := typed.Header()
		header.TargetStartMessageId = &commonpb.MessageID{WALName: commonpb.WALName_WoodPecker, Id: "invalid"}
		typed.OverwriteHeader(header)
		_, err = auto.Append(ctx, a)
		require.Error(t, err)
		require.Len(t, p.messages, 1)
		// A corrupt persisted boundary must fail active readers.
		_, err = p.Append(ctx, a)
		require.NoError(t, err)
		s, err := auto.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartAfter(first)})
		require.NoError(t, err)
		select {
		case _, ok := <-s.Chan():
			require.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("corrupt link stalled")
		}
		require.Error(t, s.Error())
		s.Close()
		for _, policy := range []*streamingpb.DeliverPolicy{
			options.DeliverPolicyStartFrom(pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 0, 0, 0))),
			options.DeliverPolicyStartAfter(pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 0, 0, 0))),
		} {
			switch v := policy.Policy.(type) {
			case *streamingpb.DeliverPolicy_StartFrom:
				v.StartFrom.Id = "bad"
			case *streamingpb.DeliverPolicy_StartAfter:
				v.StartAfter.Id = "bad"
			}
			_, err = auto.Read(ctx, walimpls.ReadOption{DeliverPolicy: policy})
			require.Error(t, err)
		}
		auto.Close()
		require.Error(t, auto.Truncate(ctx, first))
	})
}

type readOnlyLog struct{ *physicalLog }

func (b *readOnlyLog) Channel() types.PChannelInfo {
	info := b.physicalLog.Channel()
	info.AccessMode = types.AccessModeRO
	return info
}

func TestLinkedWALReadOnlyHistory(t *testing.T) {
	mockey.PatchConvey("reading historical options never acquires a writer", t, func() {
		p, w, open := installBackends(t)
		ctx := context.Background()
		writer := New(p, open)
		first, err := writer.Append(ctx, ordinary(10, nil))
		require.NoError(t, err)
		a := alter(20, w.name, first)
		_, err = writer.Append(ctx, a)
		require.NoError(t, err)
		start, err := message.WALSwitchStart(a)
		require.NoError(t, err)
		_, err = writer.Append(ctx, ordinary(30, start))
		require.NoError(t, err)
		opens := 0
		reader := New(&readOnlyLog{w}, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
			require.Equal(t, types.AccessModeRO, mode)
			require.Equal(t, p.name, name)
			opens++
			return &readOnlyLog{p}, nil
		})
		for range 2 {
			s, err := reader.Read(ctx, walimpls.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(first)})
			require.NoError(t, err)
			for _, tick := range []uint64{10, 20, 30} {
				require.Equal(t, tick, receive(t, s).TimeTick())
			}
			s.Close()
		}
		require.Equal(t, 1, opens)
		require.NoError(t, reader.Truncate(ctx, first))
		require.Nil(t, p.truncated)
		reader.Close()
	})
}

func TestRecoveredWritersTransferOwnership(t *testing.T) {
	mockey.PatchConvey("recovery handles are reused and closed exactly once", t, func() {
		p, w, _ := installBackends(t)
		closed := map[message.WALName]int{}
		mockey.Mock((*physicalLog).Close).To(func(b *physicalLog) { closed[b.name]++ }).Build()
		auto := New(w, func(context.Context, message.WALName, types.AccessMode) (walimpls.WALImpls, error) {
			t.Fatal("transferred backend must not be reopened")
			return nil, nil
		}, p, w)
		require.Equal(t, w.name, auto.WALName())
		require.NoError(t, auto.Truncate(context.Background(), pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 0, 0, 0))))
		auto.Close()
		auto.Close()
		require.Equal(t, map[message.WALName]int{p.name: 1, w.name: 1}, closed)
	})
}
