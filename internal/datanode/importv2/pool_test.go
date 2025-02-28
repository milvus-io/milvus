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

package importv2

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

	defer func() {
		_ = pt.Reset(pt.DataNodeCfg.MaxConcurrentImportTaskNum.Key)
	}()

	t.Run("ExecPool", func(t *testing.T) {
		expectedCap := pt.DataNodeCfg.MaxConcurrentImportTaskNum.GetAsInt()
		assert.Equal(t, expectedCap, GetExecPool().Cap())
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap())

		_ = pt.Save(pt.DataNodeCfg.MaxConcurrentImportTaskNum.Key, fmt.Sprintf("%d", expectedCap*2))
		expectedCap = pt.DataNodeCfg.MaxConcurrentImportTaskNum.GetAsInt()
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap())

		_ = pt.Save(pt.DataNodeCfg.MaxConcurrentImportTaskNum.Key, "0")
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap(), "pool shall not be resized when newSize is 0")

		_ = pt.Save(pt.DataNodeCfg.MaxConcurrentImportTaskNum.Key, "invalid")
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap())
	})
}
