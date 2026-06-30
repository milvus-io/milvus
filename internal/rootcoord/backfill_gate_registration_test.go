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
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fakeWatermarkRegistrar captures RegisterWatermark calls for assertions.
type fakeWatermarkRegistrar struct {
	calls []struct {
		collectionID int64
		fieldIDs     []int64
		watermark    int32
		timeTick     uint64
	}
	err error
}

func (f *fakeWatermarkRegistrar) RegisterWatermark(_ context.Context, collectionID, _ int64, fieldIDs []int64, watermark int32, timeTick uint64) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, struct {
		collectionID int64
		fieldIDs     []int64
		watermark    int32
		timeTick     uint64
	}{collectionID, fieldIDs, watermark, timeTick})
	return nil
}

func TestSetBackfillAtomicGate(t *testing.T) {
	c := &Core{}
	gate := &fakeWatermarkRegistrar{}
	c.SetBackfillAtomicGate(gate)
	assert.Equal(t, BackfillAtomicGateRegistrar(gate), c.backfillGate)
}

func TestRegisterFunctionFieldGate(t *testing.T) {
	ctx := context.Background()

	t.Run("registers a watermark round for the declared fields", func(t *testing.T) {
		gate := &fakeWatermarkRegistrar{}
		c := &Core{backfillGate: gate, idAllocator: newMockIDAllocator()}
		assert.NoError(t, c.registerFunctionFieldGate(ctx, 100, []int64{10}, 3, 77))
		assert.Len(t, gate.calls, 1)
		assert.Equal(t, int64(100), gate.calls[0].collectionID)
		assert.Equal(t, []int64{10}, gate.calls[0].fieldIDs)
		assert.Equal(t, int32(3), gate.calls[0].watermark)
		assert.Equal(t, uint64(77), gate.calls[0].timeTick)
	})

	t.Run("nil gate or empty declaration is a no-op", func(t *testing.T) {
		c := &Core{idAllocator: newMockIDAllocator()}
		assert.NoError(t, c.registerFunctionFieldGate(ctx, 100, []int64{10}, 3, 77))
		gate := &fakeWatermarkRegistrar{}
		c = &Core{backfillGate: gate, idAllocator: newMockIDAllocator()}
		assert.NoError(t, c.registerFunctionFieldGate(ctx, 100, nil, 3, 77))
		assert.Empty(t, gate.calls)
	})

	t.Run("allocation failure propagates into the ack retry loop", func(t *testing.T) {
		gate := &fakeWatermarkRegistrar{}
		alloc := newMockIDAllocator()
		alloc.AllocOneF = func() (typeutil.UniqueID, error) { return 0, errors.New("alloc down") }
		c := &Core{backfillGate: gate, idAllocator: alloc}
		assert.Error(t, c.registerFunctionFieldGate(ctx, 100, []int64{10}, 3, 77))
		assert.Empty(t, gate.calls)
	})

	t.Run("registration failure propagates into the ack retry loop", func(t *testing.T) {
		gate := &fakeWatermarkRegistrar{err: errors.New("etcd down")}
		c := &Core{backfillGate: gate, idAllocator: newMockIDAllocator()}
		assert.Error(t, c.registerFunctionFieldGate(ctx, 100, []int64{10}, 3, 77))
		assert.Empty(t, gate.calls)
	})
}
