//go:build test
// +build test

package events

type nopEventsNotifier struct{}

func (en *nopEventsNotifier) Notify(ev ...Event) {
}

// For testing.
func InitNopEventsNotifier() {
	n = &nopEventsNotifier{}
}
