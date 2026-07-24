package conc

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type SingleflightSuite struct {
	suite.Suite
}

func (s *SingleflightSuite) TestDo() {
	counter, hasShared := atomic.Int32{}, atomic.Bool{}

	sf := Singleflight[any]{}
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			_, _, shared := sf.Do("test_do", func() (any, error) {
				<-ch
				counter.Add(1)
				return struct{}{}, nil
			})
			if shared {
				hasShared.Store(true)
			}
		}(i)
	}
	close(ch)
	wg.Wait()
	if hasShared.Load() {
		s.Less(counter.Load(), int32(10))
	}
}

func (s *SingleflightSuite) TestDoChan() {
	counter, hasShared := atomic.Int32{}, atomic.Bool{}

	sf := Singleflight[any]{}
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			ch := sf.DoChan("test_dochan", func() (any, error) {
				<-ch
				counter.Add(1)
				return struct{}{}, nil
			})
			result := <-ch
			if result.Shared {
				hasShared.Store(true)
			}
		}(i)
	}
	close(ch)
	wg.Wait()
	if hasShared.Load() {
		s.Less(counter.Load(), int32(10))
	}
}

func (s *SingleflightSuite) TestDoChanDoesNotRetainGoroutinePerWaiter() {
	sf := Singleflight[any]{}
	started := make(chan struct{})
	release := make(chan struct{})

	first := sf.DoChan("test_dochan_goroutines", func() (any, error) {
		close(started)
		<-release
		return struct{}{}, nil
	})
	<-started

	baseline := runtime.NumGoroutine()
	const waiters = 2000
	for i := 0; i < waiters; i++ {
		_ = sf.DoChan("test_dochan_goroutines", func() (any, error) {
			s.FailNow("singleflight callback unexpectedly ran twice")
			return nil, nil
		})
	}

	time.Sleep(20 * time.Millisecond)
	s.Less(runtime.NumGoroutine(), baseline+50)

	close(release)
	<-first
}

func (s *SingleflightSuite) TestForget() {
	sf := Singleflight[any]{}
	ch := make(chan struct{})
	submitted := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sf.Do("test_forget", func() (any, error) {
			defer wg.Done()
			close(submitted)
			<-ch
			return struct{}{}, nil
		})
	}()

	<-submitted
	flag := false
	sf.Forget("test_forget")
	sf.Do("test_forget", func() (any, error) {
		flag = true
		return struct{}{}, nil
	})

	close(ch)
	wg.Wait()

	s.True(flag, "new job shall be executed after forget")
}

func TestSingleFlight(t *testing.T) {
	suite.Run(t, new(SingleflightSuite))
}
