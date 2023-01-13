package rootcoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_waitForTsSyncedStep_Execute(t *testing.T) {
	//Params.InitOnce()
	//Params.ProxyCfg.TimeTickInterval = time.Millisecond

	ticker := newRocksMqTtSynchronizer()
	core := newTestCore(withTtSynchronizer(ticker))
	core.chanTimeTick.syncedTtHistogram.update("ch1", 100)
	s := &waitForTsSyncedStep{
		baseStep: baseStep{core: core},
		ts:       101,
		channel:  "ch1",
	}
	children, err := s.Execute(context.Background())
	assert.Equal(t, 0, len(children))
	assert.Error(t, err)
	core.chanTimeTick.syncedTtHistogram.update("ch1", 102)
	children, err = s.Execute(context.Background())
	assert.Equal(t, 0, len(children))
	assert.NoError(t, err)
}

func restoreConfirmGCInterval() {
	confirmGCInterval = time.Minute * 20
}

func Test_confirmGCStep_Execute(t *testing.T) {
	t.Run("wait for reschedule", func(t *testing.T) {
		confirmGCInterval = time.Minute * 1000
		defer restoreConfirmGCInterval()

		s := &confirmGCStep{lastScheduledTime: time.Now()}
		_, err := s.Execute(context.TODO())
		assert.Error(t, err)
	})

	t.Run("GC not finished", func(t *testing.T) {
		broker := newMockBroker()
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool {
			return false
		}

		core := newTestCore(withBroker(broker))

		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()

		s := newConfirmGCStep(core, 100, 1000)
		time.Sleep(confirmGCInterval)

		_, err := s.Execute(context.TODO())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		broker := newMockBroker()
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool {
			return true
		}

		core := newTestCore(withBroker(broker))

		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()

		s := newConfirmGCStep(core, 100, 1000)
		time.Sleep(confirmGCInterval)

		_, err := s.Execute(context.TODO())
		assert.NoError(t, err)
	})
}
