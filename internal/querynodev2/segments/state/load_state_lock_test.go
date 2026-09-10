package state

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadStateLoadData(t *testing.T) {
	paramtable.Init()
	l := NewLoadStateLock(LoadStateOnlyMeta)
	// Test Load Data, roll back
	g, err := l.StartLoadData()
	assert.NoError(t, err)
	assert.NotNil(t, g)
	assert.Equal(t, LoadStateDataLoading, l.state)
	g.Done(errors.New("test"))
	assert.Equal(t, LoadStateOnlyMeta, l.state)

	// Test Load Data, success
	g, err = l.StartLoadData()
	assert.NoError(t, err)
	assert.NotNil(t, g)
	assert.Equal(t, LoadStateDataLoading, l.state)
	g.Done(nil)
	assert.Equal(t, LoadStateDataLoaded, l.state)

	// nothing to do with loaded.
	g, err = l.StartLoadData()
	assert.NoError(t, err)
	assert.Nil(t, g)

	for _, s := range []loadStateEnum{
		LoadStateDataLoading,
		LoadStateDataReleasing,
		LoadStateReleased,
	} {
		l.state = s
		g, err = l.StartLoadData()
		assert.Error(t, err)
		assert.Nil(t, g)
	}
}

func TestStartReleaseData(t *testing.T) {
	paramtable.Init()
	l := NewLoadStateLock(LoadStateOnlyMeta)
	// Test Release Data, nothing to do on only meta.
	g := l.StartReleaseData()
	assert.Nil(t, g)
	assert.Equal(t, LoadStateOnlyMeta, l.state)

	// roll back
	// never roll back on current using.
	l.state = LoadStateDataLoaded
	g = l.StartReleaseData()
	assert.Equal(t, LoadStateDataReleasing, l.state)
	assert.NotNil(t, g)
	g.Done(errors.New("test"))
	assert.Equal(t, LoadStateDataLoaded, l.state)

	// success
	l.state = LoadStateDataLoaded
	g = l.StartReleaseData()
	assert.Equal(t, LoadStateDataReleasing, l.state)
	assert.NotNil(t, g)
	g.Done(nil)
	assert.Equal(t, LoadStateOnlyMeta, l.state)

	// nothing to do on released
	l.state = LoadStateReleased
	g = l.StartReleaseData()
	assert.Nil(t, g)

	// test blocking.
	l.state = LoadStateOnlyMeta
	g, err := l.StartLoadData()
	assert.NoError(t, err)

	ch := make(chan struct{})
	go func() {
		g := l.StartReleaseData()
		assert.NotNil(t, g)
		g.Done(nil)
		close(ch)
	}()

	// should be blocked because on loading.
	select {
	case <-ch:
		t.Errorf("should be blocked")
	case <-time.After(500 * time.Millisecond):
	}
	// loaded finished.
	g.Done(nil)

	// release can be started.
	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("should not be blocked")
	}
	assert.Equal(t, LoadStateOnlyMeta, l.state)
}

func TestBlockUntilDataLoadedOrReleased(t *testing.T) {
	paramtable.Init()
	l := NewLoadStateLock(LoadStateOnlyMeta)
	ch := make(chan struct{})
	go func() {
		l.BlockUntilDataLoadedOrReleased()
		close(ch)
	}()
	select {
	case <-ch:
		t.Errorf("should be blocked")
	case <-time.After(10 * time.Millisecond):
	}

	g, _ := l.StartLoadData()
	g.Done(nil)
	<-ch
}

func TestStartReleaseAll(t *testing.T) {
	paramtable.Init()
	l := NewLoadStateLock(LoadStateOnlyMeta)
	// Test Release All, nothing to do on only meta.
	g := l.StartReleaseAll()
	assert.NotNil(t, g)
	assert.Equal(t, LoadStateReleased, l.state)
	g.Done(nil)
	assert.Equal(t, LoadStateReleased, l.state)

	// roll back
	// never roll back on current using.
	l.state = LoadStateDataLoaded
	g = l.StartReleaseData()
	assert.Equal(t, LoadStateDataReleasing, l.state)
	assert.NotNil(t, g)
	g.Done(errors.New("test"))
	assert.Equal(t, LoadStateDataLoaded, l.state)

	// success
	l.state = LoadStateDataLoaded
	g = l.StartReleaseAll()
	assert.Equal(t, LoadStateReleased, l.state)
	assert.NotNil(t, g)
	g.Done(nil)
	assert.Equal(t, LoadStateReleased, l.state)

	// nothing to do on released
	l.state = LoadStateReleased
	g = l.StartReleaseAll()
	assert.Nil(t, g)

	// test blocking.
	l.state = LoadStateOnlyMeta
	g, err := l.StartLoadData()
	assert.NoError(t, err)

	ch := make(chan struct{})
	go func() {
		g := l.StartReleaseAll()
		assert.NotNil(t, g)
		g.Done(nil)
		close(ch)
	}()

	// should be blocked because on loading.
	select {
	case <-ch:
		t.Errorf("should be blocked")
	case <-time.After(500 * time.Millisecond):
	}
	// loaded finished.
	g.Done(nil)

	// release can be started.
	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("should not be blocked")
	}
	assert.Equal(t, LoadStateReleased, l.state)
}

func TestStartReleaseAllWaitsAfterTimeoutUntilRefCntZero(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.Key, "0.05")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.Key)

	l := NewLoadStateLock(LoadStateDataLoaded)
	assert.True(t, l.PinIfNotReleased())

	ch := make(chan LoadStateLockGuard, 1)
	go func() {
		ch <- l.StartReleaseAll()
	}()

	select {
	case g := <-ch:
		if g != nil {
			g.Done(nil)
		}
		l.Unpin()
		t.Fatal("StartReleaseAll returned before refcnt dropped to zero")
	case <-time.After(200 * time.Millisecond):
	}

	l.Unpin()

	select {
	case g := <-ch:
		assert.NotNil(t, g)
		g.Done(nil)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StartReleaseAll did not return after refcnt dropped to zero")
	}

	assert.False(t, l.PinIfNotReleased())
}

func TestWaitOrPanic(t *testing.T) {
	paramtable.Init()

	t.Run("normal", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.Key, "600")
		defer paramtable.Get().Reset(paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.Key)

		l := NewLoadStateLock(LoadStateDataLoaded)
		executed := false

		assert.NotPanics(t, func() {
			l.waitOrPanic(func(state loadStateEnum) bool {
				return state == LoadStateDataLoaded
			}, func() { executed = true })
		})

		assert.True(t, executed)
	})

	t.Run("timeout_keeps_waiting", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.Key, "0.05")
		defer paramtable.Get().Reset(paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.Key)

		l := NewLoadStateLock(LoadStateOnlyMeta)
		executed := false
		ch := make(chan struct{})

		go func() {
			l.waitOrPanic(func(state loadStateEnum) bool {
				return state == LoadStateDataLoaded
			}, func() { executed = true })
			close(ch)
		}()

		select {
		case <-ch:
			t.Fatal("waitOrPanic returned before the predicate became ready")
		case <-time.After(200 * time.Millisecond):
		}
		assert.False(t, executed)

		g, err := l.StartLoadData()
		assert.NoError(t, err)
		assert.NotNil(t, g)
		g.Done(nil)

		select {
		case <-ch:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("waitOrPanic did not return after the predicate became ready")
		}
		assert.True(t, executed)
	})
}

func TestPinIf(t *testing.T) {
	l := NewLoadStateLock(LoadStateOnlyMeta)
	assert.True(t, l.PinIf(IsNotReleased))
	l.Unpin()
	assert.False(t, l.PinIf(IsDataLoaded))

	l = NewLoadStateLock(LoadStateDataLoaded)
	assert.True(t, l.PinIf(IsNotReleased))
	l.Unpin()
	assert.True(t, l.PinIf(IsDataLoaded))
	l.Unpin()

	l = NewLoadStateLock(LoadStateOnlyMeta)
	l.StartReleaseAll().Done(nil)
	assert.False(t, l.PinIf(IsNotReleased))
	assert.False(t, l.PinIf(IsDataLoaded))
}

func TestPin(t *testing.T) {
	l := NewLoadStateLock(LoadStateOnlyMeta)
	assert.True(t, l.PinIfNotReleased())
	l.Unpin()

	l.StartReleaseAll().Done(nil)
	assert.False(t, l.PinIfNotReleased())

	l = NewLoadStateLock(LoadStateDataLoaded)
	assert.True(t, l.PinIfNotReleased())

	ch := make(chan struct{})
	go func() {
		l.StartReleaseAll().Done(nil)
		close(ch)
	}()

	select {
	case <-ch:
		t.Errorf("should be blocked")
	case <-time.After(500 * time.Millisecond):
	}

	// should be blocked until refcnt is zero.
	assert.True(t, l.PinIfNotReleased())
	l.Unpin()
	select {
	case <-ch:
		t.Errorf("should be blocked")
	case <-time.After(500 * time.Millisecond):
	}
	l.Unpin()
	<-ch

	assert.Panics(t, func() {
		// too much unpin
		l.Unpin()
	})
}
