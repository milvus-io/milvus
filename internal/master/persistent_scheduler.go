package master

type persistenceScheduler interface {
	Enqueue(interface{}) error
	schedule(interface{}) error
	scheduleLoop()

	Start() error
	Close()
}
type MockFlushScheduler struct {
}

func (m *MockFlushScheduler) Enqueue(i interface{}) error {
	return nil
}

func (m *MockFlushScheduler) schedule(i interface{}) error {
	return nil
}

func (m *MockFlushScheduler) scheduleLoop() {
}

func (m *MockFlushScheduler) Start() error {
	return nil
}

func (m *MockFlushScheduler) Close() {
}
