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

package rootcoord

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func Test_ddlTsLockManager_GetMinDdlTs(t *testing.T) {
	t.Run("there are in-progress tasks", func(t *testing.T) {
		m := newDdlTsLockManager(nil)
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
		m := newDdlTsLockManager(tsoAllocator)
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
		m := newDdlTsLockManager(tsoAllocator)
		m.UpdateLastTs(101)
		m.inProgressCnt.Store(0)
		ts := m.GetMinDdlTs()
		assert.Equal(t, Timestamp(102), ts)
		assert.Equal(t, Timestamp(102), m.lastTs.Load())
	})
}
