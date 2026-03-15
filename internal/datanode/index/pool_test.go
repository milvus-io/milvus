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

package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestResizePools(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	pt := paramtable.Get()

	testCases := []struct {
		name  string
		pool  *indexBuildPool
		param *paramtable.ParamItem
	}{
		{"VecIndexBuildPool", vecPool, &pt.DataNodeCfg.MaxVecIndexBuildConcurrency},
		{"StandaloneIndexBuildPool", standalonePool, &pt.DataNodeCfg.StandaloneIndexBuildParallelism},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				_ = pt.Reset(tc.param.Key)
			}()

			expectedCap := tc.param.GetAsInt()
			assert.Equal(t, expectedCap, tc.pool.Get().Cap())
			tc.pool.resize(&config.Event{HasUpdated: true})
			assert.Equal(t, expectedCap, tc.pool.Get().Cap())

			_ = pt.Save(tc.param.Key, fmt.Sprintf("%d", expectedCap*2))
			expectedCap = tc.param.GetAsInt()
			tc.pool.resize(&config.Event{HasUpdated: true})
			assert.Equal(t, expectedCap, tc.pool.Get().Cap())

			_ = pt.Save(tc.param.Key, "0")
			tc.pool.resize(&config.Event{HasUpdated: true})
			assert.Equal(t, expectedCap, tc.pool.Get().Cap(), "pool shall not be resized when newSize is 0")

			_ = pt.Save(tc.param.Key, "invalid")
			tc.pool.resize(&config.Event{HasUpdated: true})
			assert.Equal(t, expectedCap, tc.pool.Get().Cap())
		})
	}
}
