package tso

import (
	"os"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

var gTestTsoAllocator *GlobalTSOAllocator

func TestGlobalTSOAllocator_All(t *testing.T) {
	etcdAddress := os.Getenv("ETCD_ADDRESS")
	if etcdAddress == "" {
		ip := funcutil.GetLocalIP()
		etcdAddress = ip + ":2379"
	}
	gTestTsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, "/test/root/kv", "tsoTest"))
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

	gTestTsoAllocator.EnableMaxLogic(false)
	t.Run("GenerateTSO2", func(t *testing.T) {
		count := 1000
		maxL := 1 << 18
		startTs, err := gTestTsoAllocator.GenerateTSO(uint32(maxL))
		step := 10
		perCount := uint32(step) << 18 // 10 ms
		assert.Nil(t, err)
		lastPhysical, lastLogical := tsoutil.ParseTS(startTs)
		for i := 0; i < count; i++ {
			ts, _ := gTestTsoAllocator.GenerateTSO(perCount)
			physical, logical := tsoutil.ParseTS(ts)
			assert.Equal(t, logical, lastLogical)
			assert.Equal(t, physical, lastPhysical.Add(time.Millisecond*time.Duration(step)))
			lastPhysical = physical
		}
	})
	gTestTsoAllocator.EnableMaxLogic(true)
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

}
