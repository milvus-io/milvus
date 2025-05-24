package events

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
)

// recoveryEventBase is the base struct for recovery events.
type recoveryEventBase struct{}

func (r recoveryEventBase) isRecoveryEvent() {
}

type PersistedItem struct {
	ShardID qviews.ShardID
	Version qviews.QueryViewVersion
	State   qviews.QueryViewState
}

type RecoveryEventPersisted struct {
	recoveryEventBase
	eventBase
	Persisted []PersistedItem
}

func (RecoveryEventPersisted) EventType() EventType {
	return EventTypeRecoveryPersisted
}
