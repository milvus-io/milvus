package io

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

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
