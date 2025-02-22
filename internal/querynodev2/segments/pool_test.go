// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestResizePools(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	pt := paramtable.Get()

	defer func() {
		pt.Reset(pt.QueryNodeCfg.MaxReadConcurrency.Key)
		pt.Reset(pt.QueryNodeCfg.CGOPoolSizeRatio.Key)
		pt.Reset(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key)
		pt.Reset(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key)
	}()

	t.Run("SQPool", func(t *testing.T) {
		expectedCap := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))

		ResizeSQPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetSQPool().Cap())

		pt.Save(pt.QueryNodeCfg.CGOPoolSizeRatio.Key, strconv.FormatFloat(pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()*2, 'f', 10, 64))
		expectedCap = int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
		ResizeSQPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetSQPool().Cap())

		pt.Save(pt.QueryNodeCfg.CGOPoolSizeRatio.Key, "0")
		ResizeSQPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetSQPool().Cap(), "pool shall not be resized when newSize is 0")
	})

	t.Run("LoadPool", func(t *testing.T) {
		expectedCap := hardware.GetCPUNum() * pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt()

		ResizeLoadPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetLoadPool().Cap())

		pt.Save(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, strconv.FormatFloat(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsFloat()*2, 'f', 10, 64))
		ResizeLoadPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetLoadPool().Cap())

		pt.Save(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, "0")
		ResizeLoadPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetLoadPool().Cap())
	})

	t.Run("WarmupPool", func(t *testing.T) {
		expectedCap := hardware.GetCPUNum() * pt.CommonCfg.LowPriorityThreadCoreCoefficient.GetAsInt()

		ResizeWarmupPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetWarmupPool().Cap())

		pt.Save(pt.CommonCfg.LowPriorityThreadCoreCoefficient.Key, strconv.FormatFloat(pt.CommonCfg.LowPriorityThreadCoreCoefficient.GetAsFloat()*2, 'f', 10, 64))
		ResizeWarmupPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetWarmupPool().Cap())

		pt.Save(pt.CommonCfg.LowPriorityThreadCoreCoefficient.Key, "0")
		ResizeWarmupPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetWarmupPool().Cap())
	})

	t.Run("BfApplyPool", func(t *testing.T) {
		expectedCap := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()

		ResizeBFApplyPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetBFApplyPool().Cap())

		pt.Save(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key, strconv.FormatFloat(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsFloat()*2, 'f', 10, 64))
		ResizeBFApplyPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetBFApplyPool().Cap())

		pt.Save(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key, "0")
		ResizeBFApplyPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetBFApplyPool().Cap())
	})

	t.Run("error_pool", func(*testing.T) {
		pool := conc.NewDefaultPool[any]()
		c := pool.Cap()

		resizePool(pool, c*2, "debug")

		assert.Equal(t, c, pool.Cap())
	})
}
