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

package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistrar_RegisterWatermark(t *testing.T) {
	ctx := context.Background()
	reg := NewBackfillAtomicGate(newFakeGateKV(), newRecordingPusher())
	require.NoError(t, reg.RegisterWatermark(ctx, 100, 1, []int64{10, 11}, 3, 42))

	rounds := reg.List()
	require.Len(t, rounds, 1)
	assert.Equal(t, ScopeWatermark, rounds[0].Scope.Kind)
	assert.Equal(t, int32(3), rounds[0].Scope.Watermark)
	assert.Equal(t, "watermark:3", rounds[0].Source)
	assert.Equal(t, uint64(42), rounds[0].Scope.SchemaChangeTimeTick)
	assert.ElementsMatch(t, []int64{10, 11}, reg.GatedFields(100))

	// The schema version identifies the DDL round: a retried registration with a
	// freshly-allocated roundID dedupes onto the existing round.
	require.NoError(t, reg.RegisterWatermark(ctx, 100, 2, []int64{10, 11}, 3, 42))
	assert.Len(t, reg.List(), 1)
	assert.Equal(t, int64(1), reg.List()[0].RoundID)
}

func TestRegistrar_RegisterExternal(t *testing.T) {
	ctx := context.Background()
	reg := NewBackfillAtomicGate(newFakeGateKV(), newRecordingPusher())
	require.NoError(t, reg.RegisterExternal(ctx, 100, 1, "backfillresult:s3://r.json", []int64{10}))

	rounds := reg.List()
	require.Len(t, rounds, 1)
	assert.Equal(t, ScopeExternal, rounds[0].Scope.Kind)
	assert.Equal(t, "backfillresult:s3://r.json", rounds[0].Source)

	// Every batch of the same commit carries the same commit-level source -> all of
	// them dedupe onto ONE round.
	require.NoError(t, reg.RegisterExternal(ctx, 100, 2, "backfillresult:s3://r.json", []int64{10}))
	assert.Len(t, reg.List(), 1)
	assert.Equal(t, int64(1), reg.List()[0].RoundID)
}
