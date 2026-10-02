package walsummary

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// transformNotifier is shared by active subscriptions of one VChannel. It owns
// no worker and allocates a change token only when a reader captures one.
type transformNotifier struct {
	refs    int
	changed chan struct{}
}

func (m *Manager) WatchTransform(vchannel string) func() {
	m.mu.Lock()
	if m.transformNotifiers == nil {
		m.transformNotifiers = make(map[string]*transformNotifier)
	}
	notifier := m.transformNotifiers[vchannel]
	if notifier == nil {
		notifier = &transformNotifier{}
		m.transformNotifiers[vchannel] = notifier
	}
	notifier.refs++
	m.mu.Unlock()
	return sync.OnceFunc(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		notifier.refs--
		if notifier.refs == 0 {
			delete(m.transformNotifiers, vchannel)
		}
	})
}

func (m *Manager) notifyTransformMessageLocked(msg message.ImmutableMessage) {
	if len(m.transformNotifiers) == 0 {
		return
	}
	if msg.VChannel() == "" || msg.IsPChannelLevel() {
		if messageutil.IsPChannelTransformBarrier(msg.MessageType()) || msg.MessageType() == message.MessageTypeRecoveryBarrier {
			m.notifyAllTransformsLocked()
		}
		return
	}
	// Recovery consumes a committed transaction as one message at the commit's
	// TimeTick. Even an insert-only transaction advances query Transform MVCC.
	if msg.MessageType() == message.MessageTypeTxn {
		msg = message.AsImmutableTxnMessage(msg).Commit()
	}
	if messageutil.AdvancesQueryTransformMVCC(msg) {
		m.notifyTransformLocked(msg.VChannel())
	}
}

func (m *Manager) notifyTransformLocked(vchannel string) {
	if notifier := m.transformNotifiers[vchannel]; notifier != nil && notifier.changed != nil {
		close(notifier.changed)
		notifier.changed = nil
	}
}

func (m *Manager) notifyAllTransformsLocked() {
	for vchannel := range m.transformNotifiers {
		m.notifyTransformLocked(vchannel)
	}
}
