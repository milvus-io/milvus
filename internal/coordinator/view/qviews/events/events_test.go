package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {
	assert.Equal(t, EventShardJoin{}.EventType(), EventTypeShardJoin)
	assert.Equal(t, EventShardReady{}.EventType(), EventTypeShardReady)
	assert.Equal(t, EventShardRequestRelease{}.EventType(), EventTypeShardRequestRelease)
	assert.Equal(t, EventShardReleaseDone{}.EventType(), EventTypeShardReleaseDone)
	assert.Equal(t, EventQVApply{}.EventType(), EventTypeQVApply)
	assert.Equal(t, EventStateTransition{}.EventType(), EventTypeStateTransition)
	assert.Equal(t, SyncerEventSent{}.EventType(), EventTypeSyncSent)
	assert.Equal(t, SyncerEventOverwrite{}.EventType(), EventTypeSyncOverwrite)
	assert.Equal(t, SyncerEventAck{}.EventType(), EventTypeSyncAck)
	assert.Equal(t, SyncerEventBalanceAttrUpdate{}.EventType(), EventTypeBalanceAttrUpdate)
	assert.Equal(t, RecoveryEventPersisted{}.EventType(), EventTypeRecoveryPersisted)
	assert.Equal(t, RecoveryEventPersisted{}.EventType(), EventTypeRecoveryPersisted)
	_ = EventTypeShardJoin.String()
	_ = EventTypeShardReady.String()
	_ = EventTypeShardRequestRelease.String()
	_ = EventTypeShardReleaseDone.String()
	_ = EventTypeQVApply.String()
	_ = EventTypeRecoveryPersisted.String()
	_ = EventTypeStateTransition.String()
	_ = EventTypeSyncSent.String()
	_ = EventTypeSyncOverwrite.String()
	_ = EventTypeSyncAck.String()
	_ = EventTypeBalanceAttrUpdate.String()
}
