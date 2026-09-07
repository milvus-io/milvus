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
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewTombstoneSweeper creates a new tombstone sweeper.
// It will start a background goroutine to sweep the tombstones periodically.
// Once the tombstone is safe to be removed, it will be removed by the background goroutine.
func NewTombstoneSweeper() TombstoneSweeper {
	ts := &tombstoneSweeperImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		interval: 5 * time.Minute,
	}
	ts.SetLogger(mlog.With(mlog.FieldModule(typeutil.RootCoordRole), mlog.FieldComponent("tombstone_sweeper")))
	go ts.background()
	return ts
}

// TombstoneSweeper is a sweeper for the tombstones.
type tombstoneSweeperImpl struct {
	mlog.Binder

	notifier *syncutil.AsyncTaskNotifier[struct{}]

	// tombstones is written by DropCollection/DropPartition callbacks and
	// iterated by the background GC concurrently. The entry wrapper gives
	// CompareAndDelete a comparable generation token, so a concurrent
	// re-registration with the same ID cannot be deleted by an older sweep.
	tombstones sync.Map // map[string]*tombstoneEntry
	interval   time.Duration
	// TODO: add metrics for the tombstone sweeper.
}

type tombstoneEntry struct {
	tombstone Tombstone
}

// AddTombstone adds a tombstone to the sweeper without waiting for the
// background GC, which may be performing slow external calls.
func (s *tombstoneSweeperImpl) AddTombstone(tombstone Tombstone) {
	if s.notifier.Context().Err() != nil {
		return
	}

	_, loaded := s.tombstones.Swap(tombstone.ID(), &tombstoneEntry{tombstone: tombstone})
	if !loaded {
		s.Logger().Info(context.TODO(), "tombstone added", mlog.String("tombstone", tombstone.ID()))
	}
}

func (s *tombstoneSweeperImpl) background() {
	defer func() {
		s.notifier.Finish(struct{}{})
		s.Logger().Info(context.TODO(), "tombstone sweeper background exit")
	}()
	s.Logger().Info(context.TODO(), "tombstone sweeper background start", mlog.Duration("interval", s.interval))

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.triggerGCTombstone(s.notifier.Context())
		case <-s.notifier.Context().Done():
			return
		}
	}
}

// triggerGCTombstone triggers the garbage collection of the tombstones.
func (s *tombstoneSweeperImpl) triggerGCTombstone(ctx context.Context) {
	s.tombstones.Range(func(key, value any) bool {
		if ctx.Err() != nil {
			// The tombstone sweeper is closing, stop it.
			return false
		}

		tombstoneID := key.(string)
		entry := value.(*tombstoneEntry)
		tombstone := entry.tombstone
		confirmed, err := tombstone.ConfirmCanBeRemoved(ctx)
		if err != nil {
			s.Logger().Warn(ctx, "fail to confirm if tombstone can be removed", mlog.String("tombstone", tombstoneID), mlog.Err(err))
			return true
		}
		if !confirmed {
			return true
		}
		if err := tombstone.Remove(ctx); err != nil {
			s.Logger().Warn(ctx, "fail to remove tombstone", mlog.String("tombstone", tombstoneID), mlog.Err(err))
			return true
		}
		if s.tombstones.CompareAndDelete(tombstoneID, entry) {
			s.Logger().Info(ctx, "tombstone removed", mlog.String("tombstone", tombstoneID))
		}
		return true
	})
}

// Close closes the tombstone sweeper.
func (s *tombstoneSweeperImpl) Close() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()
}
