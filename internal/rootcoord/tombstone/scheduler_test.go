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

package tombstone

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestTombstoneSweeper_AddTombstone(t *testing.T) {
	sweeper := NewTombstoneSweeper()
	sweeper.Close()

	sweeperImpl := &tombstoneSweeperImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		interval: 1 * time.Millisecond,
	}
	go sweeperImpl.background()

	testTombstone := &testTombstoneImpl{
		id:        "test",
		confirmed: atomic.NewBool(false),
		canRemove: atomic.NewBool(false),
		removed:   atomic.NewBool(false),
	}

	sweeperImpl.AddTombstone(testTombstone)

	time.Sleep(5 * time.Millisecond)
	assert.False(t, testTombstone.removed.Load())

	testTombstone.confirmed.Store(true)
	time.Sleep(5 * time.Millisecond)
	assert.False(t, testTombstone.removed.Load())

	testTombstone.canRemove.Store(true)
	assert.Eventually(t, func() bool {
		return testTombstone.removed.Load()
	}, 100*time.Millisecond, 10*time.Millisecond)

	sweeperImpl.Close()
	assert.Equal(t, 0, countTombstones(sweeperImpl))
}

func TestTombstoneSweeper_AddDoesNotBlockDuringGC(t *testing.T) {
	sweeper := &tombstoneSweeperImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
	}
	defer sweeper.notifier.Cancel()

	confirmStarted := make(chan struct{})
	confirmRelease := make(chan struct{})
	blocking := &funcTombstone{
		id: "blocking",
		confirm: func(ctx context.Context) (bool, error) {
			close(confirmStarted)
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-confirmRelease:
				return false, nil
			}
		},
	}
	sweeper.AddTombstone(blocking)

	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		sweeper.triggerGCTombstone(context.Background())
	}()

	select {
	case <-confirmStarted:
	case <-time.After(time.Second):
		t.Fatal("GC did not start")
	}

	addDone := make(chan struct{})
	go func() {
		defer close(addDone)
		sweeper.AddTombstone(&funcTombstone{id: "concurrent"})
	}()

	select {
	case <-addDone:
	case <-time.After(time.Second):
		t.Fatal("AddTombstone blocked while GC was running")
	}

	close(confirmRelease)
	select {
	case <-gcDone:
	case <-time.After(time.Second):
		t.Fatal("GC did not finish")
	}
	require.Equal(t, 2, countTombstones(sweeper))
}

func TestTombstoneSweeper_ConcurrentReplacementIsNotDeleted(t *testing.T) {
	sweeper := &tombstoneSweeperImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
	}
	defer sweeper.notifier.Cancel()

	replacement := &funcTombstone{id: "same"}
	original := &funcTombstone{
		id: "same",
		confirm: func(context.Context) (bool, error) {
			return true, nil
		},
		remove: func(context.Context) error {
			sweeper.AddTombstone(replacement)
			return nil
		},
	}
	sweeper.AddTombstone(original)

	sweeper.triggerGCTombstone(context.Background())

	value, ok := sweeper.tombstones.Load("same")
	require.True(t, ok)
	assert.Same(t, replacement, value.(*tombstoneEntry).tombstone)
}

func countTombstones(sweeper *tombstoneSweeperImpl) int {
	count := 0
	sweeper.tombstones.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

type testTombstoneImpl struct {
	id        string
	confirmed *atomic.Bool
	canRemove *atomic.Bool
	removed   *atomic.Bool
}

func (t *testTombstoneImpl) ID() string {
	return t.id
}

func (t *testTombstoneImpl) ConfirmCanBeRemoved(ctx context.Context) (bool, error) {
	if rand.Intn(2) == 0 {
		return false, errors.New("fail to confirm")
	}
	return t.confirmed.Load(), nil
}

func (t *testTombstoneImpl) Remove(ctx context.Context) error {
	if !t.canRemove.Load() {
		return errors.New("tombstone can not be removed")
	}
	t.removed.Store(true)
	return nil
}

type funcTombstone struct {
	id      string
	confirm func(context.Context) (bool, error)
	remove  func(context.Context) error
}

func (t *funcTombstone) ID() string {
	return t.id
}

func (t *funcTombstone) ConfirmCanBeRemoved(ctx context.Context) (bool, error) {
	if t.confirm == nil {
		return false, nil
	}
	return t.confirm(ctx)
}

func (t *funcTombstone) Remove(ctx context.Context) error {
	if t.remove == nil {
		return nil
	}
	return t.remove(ctx)
}
