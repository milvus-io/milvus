package master

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

var gTestTsoAllocator Allocator
var gTestIDAllocator *GlobalIDAllocator

func TestGlobalTSOAllocator_All(t *testing.T) {
	Init()
	gTestTsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{Params.EtcdAddress}, "/test/root/kv", "tso"))
	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{Params.EtcdAddress}, "/test/root/kv", "gid"))

	t.Run("Initialize", func(t *testing.T) {
		err := gTestTsoAllocator.Initialize()
		assert.Nil(t, err)
	})

	t.Run("GenerateTSO", func(t *testing.T) {
		count := 1000
		perCount := uint32(100)
		startTs, err := gTestTsoAllocator.GenerateTSO(perCount)
		assert.Nil(t, err)
		lastPhysical, lastLogical := tsoutil.ParseTS(startTs)
		for i := 0; i < count; i++ {
			ts, _ := gTestTsoAllocator.GenerateTSO(perCount)
			physical, logical := tsoutil.ParseTS(ts)
			if lastPhysical.Equal(physical) {
				diff := logical - lastLogical
				assert.Equal(t, uint64(perCount), diff)
			}
			lastPhysical, lastLogical = physical, logical
		}
	})

	t.Run("SetTSO", func(t *testing.T) {
		curTime := time.Now()
		nextTime := curTime.Add(2 * time.Second)
		physical := nextTime.UnixNano() / int64(time.Millisecond)
		logical := int64(0)
		err := gTestTsoAllocator.SetTSO(tsoutil.ComposeTS(physical, logical))
		assert.Nil(t, err)
	})

	t.Run("UpdateTSO", func(t *testing.T) {
		err := gTestTsoAllocator.UpdateTSO()
		assert.Nil(t, err)
	})

	t.Run("Reset", func(t *testing.T) {
		gTestTsoAllocator.Reset()
	})

	t.Run("Initialize", func(t *testing.T) {
		err := gTestIDAllocator.Initialize()
		assert.Nil(t, err)
	})

	t.Run("AllocOne", func(t *testing.T) {
		one, err := gTestIDAllocator.AllocOne()
		assert.Nil(t, err)
		ano, err := gTestIDAllocator.AllocOne()
		assert.Nil(t, err)
		assert.NotEqual(t, one, ano)
	})

	t.Run("Alloc", func(t *testing.T) {
		count := uint32(2 << 10)
		idStart, idEnd, err := gTestIDAllocator.Alloc(count)
		assert.Nil(t, err)
		assert.Equal(t, count, uint32(idEnd-idStart))
	})

}
