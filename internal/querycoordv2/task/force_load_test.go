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

package task

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A segment refused for memory ForceLoadAfterRejections times in a row is
// sent with force_load; any other outcome clears the count.
func TestExecutorForceLoadAfterRejections(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.ForceLoadAfterRejections.Key
	paramtable.Get().Save(key, "3")
	defer paramtable.Get().Reset(key)

	ex := &Executor{rejections: typeutil.NewConcurrentMap[int64, int]()}
	refused := merr.WrapErrSegmentRequestResourceFailed("Memory")

	require.False(t, ex.forceLoad(1))
	ex.noteLoadResult(1, false, refused)
	ex.noteLoadResult(1, false, refused)
	require.False(t, ex.forceLoad(1), "two refusals are not enough")
	ex.noteLoadResult(1, false, refused)
	require.True(t, ex.forceLoad(1), "the third refusal in a row forces the next load")

	// A different failure breaks the run.
	ex.noteLoadResult(1, true, errors.New("timeout"))
	require.False(t, ex.forceLoad(1))

	// So does a success, forced or not.
	for i := 0; i < 3; i++ {
		ex.noteLoadResult(2, false, refused)
	}
	require.True(t, ex.forceLoad(2))
	ex.noteLoadResult(2, true, nil)
	require.False(t, ex.forceLoad(2))

	// Disabled: never forced.
	paramtable.Get().Save(key, "0")
	for i := 0; i < 5; i++ {
		ex.noteLoadResult(3, false, refused)
	}
	require.False(t, ex.forceLoad(3))
}
