package syncmgr

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

/*
type mockTask struct {
	targetID int64
	ch       chan struct{}
	err      error
}

func (t *mockTask) done() {
	close(t.ch)
}

func (t *mockTask) SegmentID() int64                  { panic("no implementation") }
func (t *mockTask) Checkpoint() *msgpb.MsgPosition    { panic("no implementation") }
func (t *mockTask) StartPosition() *msgpb.MsgPosition { panic("no implementation") }
func (t *mockTask) ChannelName() string               { panic("no implementation") }

func (t *mockTask) Run() error {
	<-t.ch
	return t.err
}

func newMockTask(err error) *mockTask {
	return &mockTask{
		err: err,
		ch:  make(chan struct{}),
	}
}*/

type KeyLockDispatcherSuite struct {
	suite.Suite
}

func (s *KeyLockDispatcherSuite) TestKeyLock() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newKeyLockDispatcher[int64](2)

	done := make(chan struct{})
	t1 := NewMockTask(s.T())
	t1.EXPECT().Run(ctx).Run(func(_ context.Context) {
		<-done
	}).Return(nil)
	t2 := NewMockTask(s.T())
	t2.EXPECT().Run(ctx).Return(nil)

	sig := atomic.NewBool(false)

	d.Submit(ctx, 1, t1)

	go func() {
		d.Submit(ctx, 1, t2)

		sig.Store(true)
	}()

	s.False(sig.Load(), "task 2 will never be submit before task 1 done")

	close(done)

	s.Eventually(sig.Load, time.Second, time.Millisecond*100)
}

func (s *KeyLockDispatcherSuite) TestCap() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newKeyLockDispatcher[int64](1)

	t1 := NewMockTask(s.T())
	t2 := NewMockTask(s.T())

	done := make(chan struct{})
	t1.EXPECT().Run(ctx).Run(func(_ context.Context) {
		<-done
	}).Return(nil)
	t2.EXPECT().Run(ctx).Return(nil)
	sig := atomic.NewBool(false)

	d.Submit(ctx, 1, t1)

	go func() {
		// defer t2.done()
		d.Submit(ctx, 2, t2)

		sig.Store(true)
	}()

	s.False(sig.Load(), "task 2 will never be submit before task 1 done")

	close(done)

	s.Eventually(sig.Load, time.Second, time.Millisecond*100)
}

func TestKeyLockDispatcher(t *testing.T) {
	suite.Run(t, new(KeyLockDispatcherSuite))
}
