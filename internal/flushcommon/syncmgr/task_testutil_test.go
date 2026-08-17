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

package syncmgr

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
)

// runTaskForTest runs both phases the way the dispatcher does, for tests that
// only care about the end-to-end effect of a single attempt.
func runTaskForTest(ctx context.Context, task Task) error {
	if err := task.Prepare(ctx); err != nil {
		return err
	}
	return task.Commit(ctx)
}

// newSequentialIDAllocator returns an allocator whose AllocOne and Alloc share
// one sequential counter starting at next, or that fails with err when set.
func newSequentialIDAllocator(next allocator.UniqueID, err error) *allocator.MockGIDAllocator {
	a := allocator.NewMockGIDAllocator()
	a.AllocOneF = func() (allocator.UniqueID, error) {
		if err != nil {
			return 0, err
		}
		id := next
		next++
		return id, nil
	}
	a.AllocF = func(count uint32) (allocator.UniqueID, allocator.UniqueID, error) {
		if err != nil {
			return 0, 0, err
		}
		begin := next
		next += allocator.UniqueID(count)
		return begin, next, nil
	}
	return a
}

// fakeGrowingFlushSource is the one configurable GrowingFlushSource double for
// this package, mirroring the func-field shape writebuffer's tests use (see
// l0_write_buffer_test.go). Zero-value defaults describe a source that is
// fully caught up (TSafe MaxUint64) — these doubles exercise the flush path,
// not the readiness gate — reports the standard materialized layout
// {0, 1, 100, 101, 102}, and hands back 10 sequential int64 primary keys.
type fakeGrowingFlushSource struct {
	// tsafe is the consumption watermark this double reports. Zero means "far
	// ahead of anything the tests fence on"; a test that wants the source to
	// look behind sets it explicitly.
	tsafe uint64
	// rows sets how many sequential primary keys PrimaryKeys fabricates. The
	// fences are timestamps, so the double cannot derive a row count from
	// them; zero defaults to the batch size these tests build their tasks
	// with (10).
	rows        int64
	primaryKeys []storage.PrimaryKey
	primaryErr  error
	// materialized nil means the default layout; use []int64{} for a source
	// that reports nothing materialized.
	materialized []int64
	checkConfig  func(*GrowingFlushConfig)
	flushFunc    func(context.Context, uint64, uint64, *GrowingFlushConfig) (*GrowingFlushResult, error)

	// Observed side effects, for assertions.
	flushed  int
	commits  []uint64
	releases int
}

func (s *fakeGrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	if s.materialized != nil {
		return s.materialized, nil
	}
	return []int64{0, 1, 100, 101, 102}, nil
}

func (s *fakeGrowingFlushSource) PrimaryKeys(ctx context.Context, startTs, endTs uint64) ([]storage.PrimaryKey, error) {
	if s.primaryErr != nil {
		return nil, s.primaryErr
	}
	if s.primaryKeys != nil {
		return s.primaryKeys, nil
	}
	rows := s.rows
	if rows == 0 {
		rows = 10
	}
	pks := make([]storage.PrimaryKey, 0, rows)
	for i := int64(0); i < rows; i++ {
		pks = append(pks, storage.NewInt64PrimaryKey(i))
	}
	return pks, nil
}

func (s *fakeGrowingFlushSource) TSafe() uint64 {
	if s.tsafe != 0 {
		return s.tsafe
	}
	return math.MaxUint64
}

func (s *fakeGrowingFlushSource) FlushGrowingData(ctx context.Context, startTs, endTs uint64, config *GrowingFlushConfig) (*GrowingFlushResult, error) {
	s.flushed++
	if s.checkConfig != nil {
		s.checkConfig(config)
	}
	if s.flushFunc != nil {
		return s.flushFunc(ctx, startTs, endTs, config)
	}
	return &GrowingFlushResult{
		ManifestPath:  "manifest",
		NumRows:       10,
		TimestampFrom: 101,
		TimestampTo:   200,
		ColumnGroupMemorySizes: fakeColumnGroupMemorySizes(config, map[int64]int64{
			0:   80,
			101: 120,
			102: 160,
		}),
		FieldNullCounts: map[int64]int64{
			101: 2,
		},
	}, nil
}

func (s *fakeGrowingFlushSource) Release() {
	s.releases++
}

func (s *fakeGrowingFlushSource) CommitGrowingFlush(flushThroughTs uint64) {
	s.commits = append(s.commits, flushThroughTs)
}

// fakeColumnGroupMemorySizes echoes a size per column group named by the
// config, falling back to 80 for groups the sizes map does not name, so a
// double's flush result always covers exactly the layout it was asked for.
func fakeColumnGroupMemorySizes(config *GrowingFlushConfig, sizes map[int64]int64) map[int64]int64 {
	if config == nil || len(config.ColumnGroups) == 0 {
		return sizes
	}
	result := make(map[int64]int64, len(config.ColumnGroups))
	for _, columnGroup := range config.ColumnGroups {
		if size, ok := sizes[columnGroup.GroupID]; ok {
			result[columnGroup.GroupID] = size
			continue
		}
		result[columnGroup.GroupID] = 80
	}
	return result
}
