package rootcoord

import (
	"context"
	"testing"

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
