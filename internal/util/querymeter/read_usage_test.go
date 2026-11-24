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

package querymeter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestBillableBytes(t *testing.T) {
	first := &internalpb.QueryReadUsage{}
	require.NoError(t, AddColumn(first, 100, 10, 3))
	second := &internalpb.QueryReadUsage{}
	require.NoError(t, AddColumn(second, 100, 20, 5))
	require.NoError(t, AddColumn(second, 101, 4, 2))
	require.NoError(t, AddReadUsage(first, second))
	size, err := BillableBytes(first, 5)
	require.NoError(t, err)
	require.EqualValues(t, 41, size)
	require.Error(t, AddReadUsage(first, nil))
	_, err = BillableBytes(&internalpb.QueryReadUsage{DataBytes: math.MaxInt64}, 1)
	require.Error(t, err)
}
