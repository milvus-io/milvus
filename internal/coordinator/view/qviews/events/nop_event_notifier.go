//go:build test
// +build test

package events

type RecordAllEventsNotifier struct {
	AllEvents []Event
}

func (en *RecordAllEventsNotifier) Notify(ev ...Event) {
	en.AllEvents = append(en.AllEvents, ev...)
}

func (en *RecordAllEventsNotifier) Iter() *RecordAllEventsNotifierIter {
	return &RecordAllEventsNotifierIter{
		RecordAllEventsNotifier: en,
		idx:                     -1,
	}
}

// For testing.
func InitRecordAllEventsNotifier() *RecordAllEventsNotifier {
	notifier := &RecordAllEventsNotifier{
		AllEvents: make([]Event, 0),
	}
	n = notifier
	return notifier
}

type RecordAllEventsNotifierIter struct {
	*RecordAllEventsNotifier
	idx int
}

func (iter *RecordAllEventsNotifierIter) Next() bool {
	if iter.idx+1 < len(iter.AllEvents) {
		iter.idx++
		return true
	}
	return false
}

func (iter *RecordAllEventsNotifierIter) Value() Event {
	return iter.AllEvents[iter.idx]
}
