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

package pkindex

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
)

func TestTestAccessors(t *testing.T) {
	env := newTestEnv(t, droppingScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	require.False(t, idx.DirtyForTest())

	insert(t, idx, 10, 1)
	require.True(t, idx.DirtyForTest())
	values, err := idx.EngineForTest().MultiGet(context.Background(), [][]byte{codec.EncodeInt64PK(1)})
	require.NoError(t, err)
	require.NotNil(t, values[0])
}

func TestPendingTxnIDsAndDropTxn(t *testing.T) {
	env := newTestEnv(t, droppingScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ctx := context.Background()

	intent, err := idx.Decide(ctx, Request{Kind: KindInsert, VChannel: "v0", PKs: int64PKs(1), TxnID: 7})
	require.NoError(t, err)
	intent.Apply(ctx, Applied{SegmentID: 10})
	require.Equal(t, []int64{7}, idx.PendingTxnIDs())
	require.False(t, idx.DirtyForTest(), "a transaction body only records pending writes")

	idx.DropTxn(7)
	require.Empty(t, idx.PendingTxnIDs())
}
