package transformlog

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

const defaultStreamCatchupWorkers = 4

type streamLogProvider interface {
	logForStream(vchannel string) *TransformLog
	streamNotifyStateSince(seq uint64) (<-chan struct{}, uint64, []string)
	validatePChannel(pchannel string) error
}

// StreamManager owns TransformLog streams for one pchannel.
type StreamManager struct {
	pchannel string
	logs     map[string]*TransformLog

	streamMu     sync.Mutex
	streamNotify chan struct{}
	streamSeq    uint64
	streamSeqByV map[string]uint64
}

// NewStreamManager creates a TransformLog stream manager for one pchannel.
func NewStreamManager(pchannel string) *StreamManager {
	return &StreamManager{
		pchannel:     pchannel,
		logs:         make(map[string]*TransformLog),
		streamNotify: make(chan struct{}),
		streamSeqByV: make(map[string]uint64),
	}
}

func (m *StreamManager) AcquireStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	return newTransformLogStream(ctx, m, pchannel)
}

func (m *StreamManager) Register(vchannel string, log *TransformLog) {
	if vchannel == "" || log == nil {
		return
	}
	log.setStreamNotifier(func() {
		m.notify(vchannel)
	})
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	m.logs[vchannel] = log
}

func (m *StreamManager) Remove(vchannel string) {
	if vchannel == "" {
		return
	}
	m.streamMu.Lock()
	log := m.logs[vchannel]
	delete(m.logs, vchannel)
	m.notifyLocked(vchannel)
	m.streamMu.Unlock()
	if log != nil {
		log.setStreamNotifier(nil)
	}
}

func (m *StreamManager) notify(vchannel string) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	m.notifyLocked(vchannel)
}

func (m *StreamManager) notifyLocked(vchannel string) {
	m.streamSeq++
	m.streamSeqByV[vchannel] = m.streamSeq
	close(m.streamNotify)
	m.streamNotify = make(chan struct{})
}

func (m *StreamManager) logForStream(vchannel string) *TransformLog {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	return m.logs[vchannel]
}

func (m *StreamManager) validatePChannel(pchannel string) error {
	if pchannel == "" {
		return errors.Wrap(wal.ErrTransformLogInvalidReadOption, "pchannel is empty")
	}
	if m.pchannel != "" && m.pchannel != pchannel {
		return errors.Wrapf(wal.ErrTransformLogInvalidReadOption, "pchannel mismatch, expected %s, got %s", m.pchannel, pchannel)
	}
	return nil
}

func (m *StreamManager) streamNotifyStateSince(seq uint64) (<-chan struct{}, uint64, []string) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	changed := make([]string, 0)
	for vchannel, vchannelSeq := range m.streamSeqByV {
		if vchannelSeq > seq {
			changed = append(changed, vchannel)
		}
	}
	sort.Strings(changed)
	return m.streamNotify, m.streamSeq, changed
}

type streamRequestKind int

const (
	streamRequestSubscribe streamRequestKind = iota
	streamRequestCloseSubscription
	streamRequestClose
)

type streamRequest struct {
	kind           streamRequestKind
	opt            wal.TransformLogSubscriptionOption
	subscriptionID int64
	subscribeResp  chan subscribeResult
	closeResp      chan closeResult
}

type subscribeResult struct {
	sub wal.TransformLogSubscription
	err error
}

type closeResult struct {
	vchannel string
	err      error
}

type streamEventKind int

const (
	streamEventCatchupEntry streamEventKind = iota
	streamEventCatchupDone
	streamEventCatchupError
)

type streamEvent struct {
	kind           streamEventKind
	subscriptionID int64
	entry          *streamingpb.TransformLogEntry
	err            error
}

type subscriptionState int

const (
	subscriptionStateCatchingUp subscriptionState = iota
	subscriptionStateLive
	subscriptionStateClosed
)

func newTransformLogStream(ctx context.Context, provider streamLogProvider, pchannel string) (wal.TransformLogStream, error) {
	if err := provider.validatePChannel(pchannel); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx) // #nosec G118 -- cancel is owned by transformLogStream.Close/finish.
	stream := &transformLogStream{
		ctx:          ctx,
		cancel:       cancel,
		provider:     provider,
		pchannel:     pchannel,
		requests:     make(chan streamRequest),
		events:       make(chan streamEvent, 1024),
		catchupTasks: make(chan *streamSubscription, 1024),
		done:         make(chan struct{}),
		subs:         make(map[int64]*streamSubscription),
		byVChannel:   make(map[string]map[int64]*streamSubscription),
	}
	_, stream.seenNotifySeq, _ = provider.streamNotifyStateSince(0)
	for i := 0; i < defaultStreamCatchupWorkers; i++ {
		go stream.catchupWorker()
	}
	go stream.run()
	return stream, nil
}

type transformLogStream struct {
	ctx    context.Context
	cancel context.CancelFunc

	provider streamLogProvider
	pchannel string

	requests     chan streamRequest
	events       chan streamEvent
	catchupTasks chan *streamSubscription
	done         chan struct{}

	nextID        int64
	seenNotifySeq uint64
	subs          map[int64]*streamSubscription
	byVChannel    map[string]map[int64]*streamSubscription

	errMu      sync.Mutex
	err        error
	finishOnce sync.Once
}

func (s *transformLogStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	resp := make(chan subscribeResult, 1)
	if !s.sendRequest(ctx, streamRequest{
		kind:          streamRequestSubscribe,
		opt:           opt,
		subscribeResp: resp,
	}) {
		return nil, s.closedError()
	}
	select {
	case result := <-resp:
		return result.sub, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, s.closedError()
	}
}

func (s *transformLogStream) Done() <-chan struct{} {
	return s.done
}

func (s *transformLogStream) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *transformLogStream) Close() error {
	resp := make(chan closeResult, 1)
	s.sendRequest(context.Background(), streamRequest{
		kind:      streamRequestClose,
		closeResp: resp,
	})
	select {
	case result := <-resp:
		return result.err
	case <-s.done:
		return s.Error()
	}
}

func (s *transformLogStream) sendRequest(ctx context.Context, req streamRequest) bool {
	select {
	case s.requests <- req:
		return true
	case <-ctx.Done():
		return false
	case <-s.done:
		return false
	}
}

func (s *transformLogStream) run() {
	defer close(s.done)
	defer close(s.catchupTasks)
	for {
		notifyCh, notifySeq, changedVChannels := s.provider.streamNotifyStateSince(s.seenNotifySeq)
		if notifySeq != s.seenNotifySeq {
			s.seenNotifySeq = notifySeq
			s.dispatchChangedLive(changedVChannels)
			continue
		}
		select {
		case req := <-s.requests:
			if s.handleRequest(req) {
				return
			}
		case event := <-s.events:
			s.handleEvent(event)
		case <-notifyCh:
		case <-s.ctx.Done():
			s.finish(s.ctx.Err())
			return
		}
	}
}

func (s *transformLogStream) handleRequest(req streamRequest) bool {
	switch req.kind {
	case streamRequestSubscribe:
		sub, err := s.createSubscription(req.opt)
		req.subscribeResp <- subscribeResult{sub: sub, err: err}
	case streamRequestCloseSubscription:
		vchannel, err := s.closeSubscription(req.subscriptionID, nil)
		req.closeResp <- closeResult{vchannel: vchannel, err: err}
	case streamRequestClose:
		s.finish(nil)
		req.closeResp <- closeResult{err: s.Error()}
		return true
	}
	return false
}

func (s *transformLogStream) createSubscription(opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if opt.Handler == nil {
		return nil, errors.Wrap(wal.ErrTransformLogInvalidReadOption, "handler is nil")
	}
	if opt.VChannel == "" {
		return nil, errors.Wrap(wal.ErrTransformLogInvalidReadOption, "vchannel is empty")
	}
	log := s.provider.logForStream(opt.VChannel)
	if log == nil {
		return nil, errors.Wrap(wal.ErrTransformLogVChannelUnavailable, "transform log is not found")
	}
	transformLog := log
	transformLog.mu.Lock()
	if opt.StartAfterTimeTick < transformLog.meta.GetTruncateTimeTick() {
		transformLog.mu.Unlock()
		return nil, errors.Wrap(wal.ErrTransformLogStartPointTruncated, "start point is truncated")
	}
	caughtUpTarget := transformLog.latestTimeTickLocked()
	transformLog.mu.Unlock()
	if opt.EndTimeTick > 0 && opt.EndTimeTick < caughtUpTarget {
		caughtUpTarget = opt.EndTimeTick
	}

	subscriptionID := opt.SubscriptionID
	if subscriptionID == 0 {
		s.nextID++
		subscriptionID = s.nextID
	}
	if old := s.subs[subscriptionID]; old != nil {
		s.finishSubscription(old, nil, false)
	}
	ctx, cancel := context.WithCancel(s.ctx) // #nosec G118 -- cancel is owned by streamSubscription.Close/finishSubscription.
	sub := &streamSubscription{
		stream:         s,
		id:             subscriptionID,
		vchannel:       opt.VChannel,
		startAfter:     opt.StartAfterTimeTick,
		end:            opt.EndTimeTick,
		cursor:         opt.StartAfterTimeTick,
		caughtUpTarget: caughtUpTarget,
		handler:        opt.Handler,
		log:            transformLog,
		state:          subscriptionStateCatchingUp,
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
	s.subs[subscriptionID] = sub
	select {
	case s.catchupTasks <- sub:
		return sub, nil
	case <-s.ctx.Done():
		s.finishSubscription(sub, s.ctx.Err(), false)
		return nil, s.ctx.Err()
	}
}

func (s *transformLogStream) handleEvent(event streamEvent) {
	sub := s.subs[event.subscriptionID]
	if sub == nil || sub.state != subscriptionStateCatchingUp {
		return
	}
	switch event.kind {
	case streamEventCatchupEntry:
		if event.entry.GetTimeTick() > sub.cursor {
			if err := sub.handler.Handle(wal.TransformLogStreamEvent{
				SubscriptionID: sub.id,
				VChannel:       sub.vchannel,
				Entry:          event.entry,
			}); err != nil {
				s.finishSubscription(sub, err, true)
				return
			}
			sub.cursor = event.entry.GetTimeTick()
		}
	case streamEventCatchupDone:
		if err := sub.handler.Handle(wal.TransformLogStreamEvent{
			SubscriptionID: sub.id,
			VChannel:       sub.vchannel,
			CaughtUp:       &wal.TransformLogCaughtUp{StartAfterTimeTick: sub.startAfter},
		}); err != nil {
			s.finishSubscription(sub, err, true)
			return
		}
		if sub.end > 0 {
			s.finishSubscription(sub, nil, false)
			return
		}
		sub.state = subscriptionStateLive
		if s.byVChannel[sub.vchannel] == nil {
			s.byVChannel[sub.vchannel] = make(map[int64]*streamSubscription)
		}
		s.byVChannel[sub.vchannel][sub.id] = sub
		s.dispatchVChannel(sub.vchannel)
	case streamEventCatchupError:
		s.finishSubscription(sub, event.err, true)
	}
}

func (s *transformLogStream) catchupWorker() {
	for sub := range s.catchupTasks {
		s.catchup(sub)
	}
}

func (s *transformLogStream) catchup(sub *streamSubscription) {
	cursor := sub.startAfter
	for {
		entry, ok, err := sub.log.nextEntryAfter(sub.ctx, cursor)
		if err != nil {
			s.sendEvent(sub.ctx, streamEvent{kind: streamEventCatchupError, subscriptionID: sub.id, err: err})
			return
		}
		if !ok {
			s.sendEvent(sub.ctx, streamEvent{kind: streamEventCatchupDone, subscriptionID: sub.id})
			return
		}
		timeTick := entry.GetTimeTick()
		if timeTick > sub.caughtUpTarget {
			s.sendEvent(sub.ctx, streamEvent{kind: streamEventCatchupDone, subscriptionID: sub.id})
			return
		}
		if !s.sendEvent(sub.ctx, streamEvent{kind: streamEventCatchupEntry, subscriptionID: sub.id, entry: entry}) {
			return
		}
		cursor = timeTick
		if cursor >= sub.caughtUpTarget {
			s.sendEvent(sub.ctx, streamEvent{kind: streamEventCatchupDone, subscriptionID: sub.id})
			return
		}
	}
}

func (s *transformLogStream) sendEvent(ctx context.Context, event streamEvent) bool {
	select {
	case s.events <- event:
		return true
	case <-ctx.Done():
		return false
	case <-s.done:
		return false
	}
}

func (s *transformLogStream) dispatchAllLive() {
	for vchannel := range s.byVChannel {
		s.dispatchVChannel(vchannel)
	}
}

func (s *transformLogStream) dispatchChangedLive(vchannels []string) {
	for _, vchannel := range vchannels {
		if vchannel == "" {
			s.dispatchAllLive()
			continue
		}
		s.dispatchVChannel(vchannel)
	}
}

func (s *transformLogStream) dispatchVChannel(vchannel string) {
	for {
		subs := s.byVChannel[vchannel]
		if len(subs) == 0 {
			delete(s.byVChannel, vchannel)
			return
		}
		if s.provider.logForStream(vchannel) == nil {
			err := errors.Wrap(wal.ErrTransformLogVChannelUnavailable, "transform log is removed")
			for _, sub := range subs {
				s.finishSubscription(sub, err, true)
			}
			continue
		}
		var log *TransformLog
		minCursor := uint64(0)
		first := true
		for _, sub := range subs {
			if sub.state != subscriptionStateLive {
				continue
			}
			log = sub.log
			if first || sub.cursor < minCursor {
				minCursor = sub.cursor
				first = false
			}
		}
		if first || log == nil {
			delete(s.byVChannel, vchannel)
			return
		}
		entry, ok, err := log.nextEntryAfter(s.ctx, minCursor)
		if err != nil {
			for _, sub := range subs {
				s.finishSubscription(sub, err, true)
			}
			return
		}
		if !ok {
			return
		}
		timeTick := entry.GetTimeTick()
		for _, sub := range subs {
			if sub.state != subscriptionStateLive || timeTick <= sub.cursor {
				continue
			}
			if sub.end > 0 && timeTick > sub.end {
				s.finishSubscription(sub, nil, false)
				continue
			}
			if err := sub.handler.Handle(wal.TransformLogStreamEvent{
				SubscriptionID: sub.id,
				VChannel:       sub.vchannel,
				Entry:          entry,
			}); err != nil {
				s.finishSubscription(sub, err, true)
				continue
			}
			sub.cursor = timeTick
		}
	}
}

func (s *transformLogStream) closeSubscription(subscriptionID int64, err error) (string, error) {
	sub := s.subs[subscriptionID]
	if sub == nil {
		return "", nil
	}
	vchannel := sub.vchannel
	s.finishSubscription(sub, err, err != nil)
	return vchannel, nil
}

func (s *transformLogStream) finishSubscription(sub *streamSubscription, err error, notifyError bool) {
	if sub.state == subscriptionStateClosed {
		return
	}
	sub.state = subscriptionStateClosed
	delete(s.subs, sub.id)
	if byID := s.byVChannel[sub.vchannel]; byID != nil {
		delete(byID, sub.id)
		if len(byID) == 0 {
			delete(s.byVChannel, sub.vchannel)
		}
	}
	if err != nil {
		sub.setError(err)
	}
	sub.cancel()
	if notifyError && err != nil {
		_ = sub.handler.Handle(wal.TransformLogStreamEvent{
			SubscriptionID: sub.id,
			VChannel:       sub.vchannel,
			Err:            err,
		})
	}
	sub.handler.Close()
	close(sub.done)
}

func (s *transformLogStream) finish(err error) {
	s.finishOnce.Do(func() {
		if err != nil && errors.Is(err, context.Canceled) {
			err = nil
		}
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		s.cancel()
		for _, sub := range s.subs {
			s.finishSubscription(sub, err, false)
		}
	})
}

func (s *transformLogStream) closedError() error {
	if err := s.Error(); err != nil {
		return err
	}
	return io.EOF
}

type streamSubscription struct {
	stream         *transformLogStream
	id             int64
	vchannel       string
	startAfter     uint64
	end            uint64
	cursor         uint64
	caughtUpTarget uint64
	handler        wal.TransformLogEventHandler
	log            *TransformLog
	state          subscriptionState
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}

	errMu sync.Mutex
	err   error
}

func (s *streamSubscription) ID() int64 {
	return s.id
}

func (s *streamSubscription) VChannel() string {
	return s.vchannel
}

func (s *streamSubscription) Close() error {
	resp := make(chan closeResult, 1)
	if !s.stream.sendRequest(context.Background(), streamRequest{
		kind:           streamRequestCloseSubscription,
		subscriptionID: s.id,
		closeResp:      resp,
	}) {
		return s.Error()
	}
	select {
	case result := <-resp:
		if result.err != nil {
			return result.err
		}
		return s.Error()
	case <-s.done:
		return s.Error()
	case <-s.stream.done:
		return s.Error()
	}
}

func (s *streamSubscription) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *streamSubscription) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}
