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

package segments

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Native DropIndex cannot be interrupted by the RPC context. Keep that
// behavior at the cgo seam while exercising the real LocalSegment metadata.
type blockedIndexDrop struct {
	segcore.CSegment
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockedIndexDrop) DropIndex(context.Context, int64) error {
	s.calls.Inc()
	s.entered <- struct{}{}
	<-s.release
	return nil
}

func TestDropIndexSerializesCanceledRPCs(t *testing.T) {
	native := &blockedIndexDrop{entered: make(chan struct{}, 2), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(native.release) }) }
	t.Cleanup(release)
	collection := &Collection{}
	collection.schema.Store(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_Int64},
	}})
	segment := &LocalSegment{
		baseSegment:  baseSegment{collection: collection},
		ptrLock:      state.NewLoadStateLock(state.LoadStateDataLoaded),
		csegment:     native,
		fieldIndexes: typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),
		indexDropSem: semaphore.NewWeighted(1),
	}
	segment.fieldIndexes.Insert(1000, &IndexedFieldInfo{IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: 1000}})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first := make(chan error, 1)
	go func() { first <- segment.DropIndex(ctx, 1000) }()
	select {
	case <-native.entered:
	case <-time.After(time.Second):
		t.Fatal("first native drop did not start")
	}
	cancel()
	second := make(chan error, 1)
	go func() { second <- segment.DropIndex(context.Background(), 1000) }()
	// A retry must not capture A's field ID while A's original native drop
	// is still running; otherwise it could later erase replacement index B.
	select {
	case <-native.entered:
		t.Fatal("duplicate drop entered native code before the first removed A's metadata")
	case <-time.After(100 * time.Millisecond):
	}
	// A canceled waiter must return without waiting for the native operation
	// to finish, otherwise repeated RPC timeouts retain pinned handlers.
	waitCtx, cancelWait := context.WithCancel(context.Background())
	t.Cleanup(cancelWait)
	waiter := make(chan error, 1)
	go func() { waiter <- segment.DropIndex(waitCtx, 1000) }()
	cancelWait()
	select {
	case err := <-waiter:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled retry remained blocked behind the native drop")
	}
	release()
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.Equal(t, int32(1), native.calls.Load())
	require.False(t, segment.fieldIndexes.Contain(1000))
	segment.fieldIndexes.Insert(1001, &IndexedFieldInfo{IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: 1001}})
	require.NoError(t, segment.DropIndex(context.Background(), 1000))
	require.Equal(t, int32(1), native.calls.Load(), "a delayed A request must not erase B by field ID")
	require.True(t, segment.fieldIndexes.Contain(1001))
	// An already-canceled retry must not start another native operation.
	require.ErrorIs(t, segment.DropIndex(ctx, 1001), context.Canceled)
	require.Equal(t, int32(1), native.calls.Load())
	require.True(t, segment.fieldIndexes.Contain(1001))
}
