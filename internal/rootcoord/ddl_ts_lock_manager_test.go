package rootcoord

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ddlTsLockManager_GetMinDdlTs(t *testing.T) {
	t.Run("there are in-progress tasks", func(t *testing.T) {
		m := newDdlTsLockManagerV2(nil)
		m.UpdateLastTs(100)
		m.inProgressCnt.Store(9999)
		ts := m.GetMinDdlTs()
		assert.Equal(t, Timestamp(100), ts)
	})

	t.Run("failed to generate ts", func(t *testing.T) {
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 0, errors.New("error mock GenerateTSO")
		}
		m := newDdlTsLockManagerV2(tsoAllocator)
		m.UpdateLastTs(101)
		m.inProgressCnt.Store(0)
		ts := m.GetMinDdlTs()
		assert.Equal(t, Timestamp(101), ts)
	})

	t.Run("normal case", func(t *testing.T) {
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 102, nil
		}
		m := newDdlTsLockManagerV2(tsoAllocator)
		m.UpdateLastTs(101)
		m.inProgressCnt.Store(0)
		ts := m.GetMinDdlTs()
		assert.Equal(t, Timestamp(102), ts)
		assert.Equal(t, Timestamp(102), m.lastTs.Load())
	})
}
