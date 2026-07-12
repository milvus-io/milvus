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

package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSchemaReadyState_PublishThenLoad(t *testing.T) {
	st := &schemaReadyState{}
	snap := &readySnapshot{version: 1}

	st.publish(snap)

	assert.Same(t, snap, st.load())
}

func TestSchemaReadyState_ResolveNotReadyWhenNil(t *testing.T) {
	st := &schemaReadyState{}

	snap, err := st.resolve()

	assert.Nil(t, snap)
	assert.ErrorIs(t, err, merr.ErrCollectionSchemaVersionNotReady)
}

func TestSchemaReadyState_ResolveReturnsSnapshot(t *testing.T) {
	st := &schemaReadyState{}
	published := &readySnapshot{version: 3}
	st.publish(published)

	snap, err := st.resolve()

	assert.NoError(t, err)
	assert.Same(t, published, snap)
}

func TestReadySnapshot_ValidateReadSchemaVersion(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}
	snap := newReadySnapshot(3, 0, schema, nil)

	// Legacy proxy (no version attached) and requests at or below the ready
	// version are served.
	assert.NoError(t, validateReadSchemaVersion(snap, 0))
	assert.NoError(t, validateReadSchemaVersion(snap, 2))
	assert.NoError(t, validateReadSchemaVersion(snap, 3))

	// A request compiled against a newer schema version means this delegator is
	// merely behind (a transient catch-up window), so the read gate reports
	// retriable NotReady, not a permanent InputError mismatch.
	err := validateReadSchemaVersion(snap, 4)
	assert.ErrorIs(t, err, merr.ErrCollectionSchemaVersionNotReady)
}

func TestSchemaReadyState_PublishIsMonotonic(t *testing.T) {
	st := &schemaReadyState{}
	newer := &readySnapshot{version: 2}
	older := &readySnapshot{version: 1}

	st.publish(newer)
	st.publish(older) // stale: must be ignored, no rollback

	assert.Same(t, newer, st.load())
}

func TestSchemaReadyState_PublishSameVersionNewerBarrierWins(t *testing.T) {
	st := &schemaReadyState{}
	v1 := &readySnapshot{version: 1, barrierTs: 10}
	v1Refresh := &readySnapshot{version: 1, barrierTs: 20} // same version, newer barrier (e.g. properties refresh)

	require.True(t, st.publish(v1))
	require.True(t, st.publish(v1Refresh), "same-version newer-barrier refresh must publish")
	assert.Same(t, v1Refresh, st.load())
}

func TestSchemaReadyState_PublishSameVersionOlderOrEqualBarrierDropped(t *testing.T) {
	st := &schemaReadyState{}
	cur := &readySnapshot{version: 1, barrierTs: 20}
	require.True(t, st.publish(cur))

	// equal barrier: dropped (no rollback, no redundant churn).
	assert.False(t, st.publish(&readySnapshot{version: 1, barrierTs: 20}))
	// older barrier at the same version: dropped.
	assert.False(t, st.publish(&readySnapshot{version: 1, barrierTs: 10}))
	assert.Same(t, cur, st.load())
}

func TestSchemaReadyState_PublishNewerVersionWinsRegardlessOfBarrier(t *testing.T) {
	st := &schemaReadyState{}
	cur := &readySnapshot{version: 1, barrierTs: 100}
	require.True(t, st.publish(cur))

	// A higher logical version wins even with a smaller barrier (barrier only
	// disambiguates within the same version).
	next := &readySnapshot{version: 2, barrierTs: 1}
	require.True(t, st.publish(next))
	assert.Same(t, next, st.load())
}
