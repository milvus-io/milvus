package io

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGetOrCreateIOPool(t *testing.T) {
	paramtable.Init()
	ioConcurrency := paramtable.Get().DataNodeCfg.IOConcurrency.GetValue()
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.IOConcurrency.Key, "64")
	defer func() { paramtable.Get().Save(paramtable.Get().DataNodeCfg.IOConcurrency.Key, ioConcurrency) }()
	nP := 10
	nTask := 10
	wg := sync.WaitGroup{}
	for i := 0; i < nP; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := GetOrCreateIOPool()
			futures := make([]*conc.Future[any], 0, nTask)
			for j := 0; j < nTask; j++ {
				future := p.Submit(func() (interface{}, error) {
					return nil, nil
				})
				futures = append(futures, future)
			}
			err := conc.AwaitAll(futures...)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestResizePools(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	defer func() {
		pt.Reset(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key)
	}()

	t.Run("BfApplyPool", func(t *testing.T) {
		expectedCap := hardware.GetCPUNum() * pt.DataNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()

		ResizeBFApplyPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetBFApplyPool().Cap())

		pt.Save(pt.DataNodeCfg.BloomFilterApplyParallelFactor.Key, strconv.FormatFloat(pt.DataNodeCfg.BloomFilterApplyParallelFactor.GetAsFloat()*2, 'f', 10, 64))
		ResizeBFApplyPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetBFApplyPool().Cap())

		pt.Save(pt.DataNodeCfg.BloomFilterApplyParallelFactor.Key, "0")
		ResizeBFApplyPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetBFApplyPool().Cap())
	})
}
