package syncmgr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type mockTask struct {
	ch  chan struct{}
	err error
}

func (t *mockTask) done() {
	close(t.ch)
}

func (t *mockTask) Run() error {
	<-t.ch
	return t.err
}

func newMockTask(err error) *mockTask {
	return &mockTask{
		err: err,
		ch:  make(chan struct{}),
	}
}

type KeyLockDispatcherSuite struct {
	suite.Suite
}

func (s *KeyLockDispatcherSuite) TestKeyLock() {
	d := newKeyLockDispatcher[int64](2)

	t1 := newMockTask(nil)
	t2 := newMockTask(nil)

	sig := atomic.NewBool(false)

	d.Submit(1, t1)

	go func() {
		defer t2.done()
		d.Submit(1, t2)

		sig.Store(true)
	}()

	s.False(sig.Load(), "task 2 will never be submit before task 1 done")

	t1.done()

	s.Eventually(sig.Load, time.Second, time.Millisecond*100)
}

func (s *KeyLockDispatcherSuite) TestCap() {
	d := newKeyLockDispatcher[int64](1)

	t1 := newMockTask(nil)
	t2 := newMockTask(nil)
	sig := atomic.NewBool(false)

	d.Submit(1, t1)

	go func() {
		defer t2.done()
		d.Submit(2, t2)

		sig.Store(true)
	}()

	s.False(sig.Load(), "task 2 will never be submit before task 1 done")

	t1.done()

	s.Eventually(sig.Load, time.Second, time.Millisecond*100)
}

func TestKeyLockDispatcher(t *testing.T) {
	suite.Run(t, new(KeyLockDispatcherSuite))
}
