package events

var n eventNotifier

type eventNotifier interface {
	Notify(ev ...Event)
}

// Notify will notify events are notified to all observers as a batch.
func Notify(ev ...Event) {
	n.Notify(ev...)
}
