package events

import (
	"time"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
)

type (
	EventType        int
	ShardID          = qviews.ShardID
	QueryViewVersion = qviews.QueryViewVersion
	QueryViewState   = qviews.QueryViewState
	StateTransition  = qviews.StateTransition
)

const (
	EventTypeShardJoin           EventType = iota // The event for a new shard join.
	EventTypeShardReady                           // The event for the first view of shard up.
	EventTypeShardRequestRelease                  // The event for shard request release.
	EventTypeShardReleaseDone                     // The event for shard release done.
	EventTypeQVApply                              // The event for query view apply.

	EventTypeRecoveryPersisted // the event when the qview is presisted by recovery storage.

	EventTypeStateTransition // The event for state transition in state machine.

	EventTypeSyncSent          // The event for sync, when sync operation is sent.
	EventTypeSyncOverwrite     // The event for overwrite, when the acknowledge or sync is not done but the state is transitted.
	EventTypeSyncAck           // The event for acked, when first acknowledge returned.
	EventTypeBalanceAttrUpdate // The event for balance, when the balance info is reported.
)

var (
	_ Event = EventStateTransition{}

	_ RecoveryEvent = RecoveryEventPersisted{}

	_ SyncerEvent = SyncerEventSent{}
	_ SyncerEvent = SyncerEventOverwrite{}
	_ SyncerEvent = SyncerEventAck{}
	_ SyncerEvent = SyncerEventBalanceAttrUpdate{}
)

// Event is the common interface for all events that can be watched.
type Event interface {
	// EventType returns the type of the event.
	EventType() EventType

	// The instant that the event happens.
	Instant() time.Time
}

// SyncerEvent is the interface for syncer events.
type SyncerEvent interface {
	Event

	isSyncerEvent()
}

// RecoveryEvent is the interface for recovery events.
type RecoveryEvent interface {
	Event

	isRecoveryEvent()
}

func (t EventType) String() string {
	switch t {
	case EventTypeShardJoin:
		return "ShardJoin"
	case EventTypeShardReady:
		return "ShardReady"
	case EventTypeShardRequestRelease:
		return "ShardRequestRelease"
	case EventTypeShardReleaseDone:
		return "ShardReleaseDone"
	case EventTypeRecoveryPersisted:
		return "RecoveryPersisted"
	case EventTypeQVApply:
		return "QVApply"
	case EventTypeStateTransition:
		return "StateTransition"
	case EventTypeSyncSent:
		return "SyncSent"
	case EventTypeSyncOverwrite:
		return "SyncOverwrite"
	case EventTypeSyncAck:
		return "SyncAck"
	case EventTypeBalanceAttrUpdate:
		return "BalanceAttrUpdate"
	default:
		panic("unknown event type")
	}
}

// eventBase is the basic event implementation.
type eventBase struct {
	instant time.Time
}

// Instant returns the instant that the event happens.
func (eb eventBase) Instant() time.Time {
	return eb.instant
}
