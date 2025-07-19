package events

import "github.com/milvus-io/milvus/internal/coordinator/view/qviews"

type EventShardJoin struct {
	eventBase
}

func (EventShardJoin) EventType() EventType {
	return EventTypeShardJoin
}

type EventShardReady struct {
	eventBase
}

func (EventShardReady) EventType() EventType {
	return EventTypeShardReady
}

type EventShardRequestRelease struct {
	eventBase
	ShardID qviews.ShardID
}

func (EventShardRequestRelease) EventType() EventType {
	return EventTypeShardRequestRelease
}

type EventShardReleaseDone struct {
	eventBase
}

func (EventShardReleaseDone) EventType() EventType {
	return EventTypeShardReleaseDone
}

type EventQVApply struct {
	eventBase
	Version QueryViewVersion
}

func (EventQVApply) EventType() EventType {
	return EventTypeQVApply
}

// EventStateTransition is the event for state transition.
type EventStateTransition struct {
	eventBase
	ShardID    ShardID
	Version    QueryViewVersion
	Transition StateTransition
}

// EventType returns the event type.
func (EventStateTransition) EventType() EventType {
	return EventTypeStateTransition
}
