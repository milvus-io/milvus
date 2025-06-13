package events

import "github.com/milvus-io/milvus/internal/coordinator/view/qviews"

type SyncerViewEventBase struct {
	eventBase
}

func (s SyncerViewEventBase) isSyncerEvent() {}

type SyncerEventSent struct {
	SyncerViewEventBase
	View qviews.QueryViewAtWorkNode
}

func (s SyncerEventSent) EventType() EventType {
	return EventTypeSyncSent
}

type SyncerEventOverwrite struct {
	SyncerViewEventBase
	PreviousView qviews.QueryViewAtWorkNode
	CurrentView  qviews.QueryViewAtWorkNode
}

func (s SyncerEventOverwrite) EventType() EventType {
	return EventTypeSyncOverwrite
}

type SyncerEventAck struct {
	SyncerViewEventBase
	AcknowledgedView qviews.QueryViewAtWorkNode
}

func (s SyncerEventAck) EventType() EventType {
	return EventTypeSyncAck
}

type SyncerEventBalanceAttrUpdate struct {
	eventBase
	BalanceAttr qviews.BalanceAttrAtWorkNode
}

func (s SyncerEventBalanceAttrUpdate) isSyncerEvent() {}

func (s SyncerEventBalanceAttrUpdate) EventType() EventType {
	return EventTypeBalanceAttrUpdate
}
