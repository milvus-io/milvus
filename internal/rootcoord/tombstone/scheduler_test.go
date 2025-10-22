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
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestTombstoneSweeper_AddTombstone(t *testing.T) {
	sweeper := NewTombstoneSweeper()
	sweeper.Close()

	sweeperImpl := &tombstoneSweeperImpl{
		notifier:   syncutil.NewAsyncTaskNotifier[struct{}](),
		incoming:   make(chan Tombstone),
		tombstones: make(map[string]Tombstone),
		interval:   1 * time.Millisecond,
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
	assert.Len(t, sweeperImpl.tombstones, 0)
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
