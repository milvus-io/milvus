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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

func TestResourceFromSlot(t *testing.T) {
	capacity := taskcommon.Resource{CPU: 8, Memory: 1024}
	require.Equal(t, taskcommon.Resource{CPU: 2, Memory: 256}, ResourceFromSlot(16, 64, capacity))
	require.Equal(t, taskcommon.Resource{CPU: 1, Memory: 16}, ResourceFromSlot(1, 64, capacity))
	require.Equal(t, capacity, ResourceFromSlot(128, 64, capacity))
	require.Equal(t, taskcommon.Resource{CPU: 1, Memory: 1}, ResourceFromSlot(1, 64, taskcommon.Resource{CPU: 1, Memory: 1}))
}
