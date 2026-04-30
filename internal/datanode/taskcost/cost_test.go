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

package taskcost

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

func TestNowMs(t *testing.T) {
	before := time.Now().UnixMilli()
	got := NowMs()
	after := time.Now().UnixMilli()
	assert.GreaterOrEqual(t, got, before)
	assert.LessOrEqual(t, got, after)
}

func TestCalcCostTimeMs(t *testing.T) {
	assert.Equal(t, int64(50), CalcCostTimeMs(100, 150))
	assert.Equal(t, int64(0), CalcCostTimeMs(100, 100))
	assert.Equal(t, int64(0), CalcCostTimeMs(0, 150), "zero start returns 0")
	assert.Equal(t, int64(0), CalcCostTimeMs(100, 0), "zero end returns 0")
	assert.Equal(t, int64(0), CalcCostTimeMs(200, 100), "end before start returns 0")
}

func TestEstimateIndexBuildCPUNum(t *testing.T) {
	assert.Equal(t, int64(hardware.GetCPUNum()), EstimateIndexBuildCPUNum(true),
		"vector index saturates knowhere build pool")
	assert.Equal(t, int64(1), EstimateIndexBuildCPUNum(false),
		"all scalar indexes currently build single-threaded")
}
