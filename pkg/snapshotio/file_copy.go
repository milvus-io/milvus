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

package snapshotio

import (
	"context"
	"math"
	"sync"

	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewTaskCopyPermits creates an independent budget for one snapshot task.
// Older requests without a positive limit use the default of 128.
func NewTaskCopyPermits(concurrency int32) chan struct{} {
	if concurrency <= 0 {
		concurrency = 128
	}
	return make(chan struct{}, int(concurrency))
}

// FileCopy maps one source object to its destination. Bucket selection and copy
// semantics belong to the caller.
type FileCopy struct {
	SourcePath      string
	DestinationPath string
	// Export metadata counts as completed files, but is excluded from byte statistics.
	IsMetadata bool
}

// DeduplicateFileCopies prepares a physical copy plan for one source/target bucket
// pair. Callers validate bucket ownership and generate destination paths first.
// Equal destinations must name the same normalized source object; the same source
// may have multiple destinations. Keep the first source spelling so the copy
// layer normalizes URIs exactly once. Destination keys are compared verbatim.
// The caller owns items: this function compacts its backing array in place.
// On conflict it returns no usable plan, before any copy can start.
func DeduplicateFileCopies(items []FileCopy) ([]FileCopy, error) {
	byDestination := make(map[string]string, len(items))
	unique := items[:0]
	for _, item := range items {
		source := NormalizeSnapshotObjectPath(item.SourcePath)
		if previous, exists := byDestination[item.DestinationPath]; exists {
			if previous != source {
				return nil, merr.WrapErrDataIntegrityMsg("copy plan maps different source objects to one destination")
			}
			continue
		}
		byDestination[item.DestinationPath] = source
		unique = append(unique, item)
	}
	clear(items[len(unique):])
	return unique, nil
}

// CopyFiles executes a plan with bounded concurrency. Sharing permits limits
// active operations across plans, tasks and segments. The callback may run
// concurrently and must record confirmed writes even if cancellation races
// completion. On error, queued work is canceled and all active callbacks are
// joined before returning, so callers can safely publish results or clean up.
func CopyFiles(ctx context.Context, items []FileCopy, permits chan struct{}, copyFile func(context.Context, FileCopy) error) error {
	return objectstore.RunBounded(ctx, items, permits, copyFile)
}

// PlanFileCopies maps a complete segment inventory before validating destination
// collisions. Mapping errors return no executable plan and perform no I/O.
func PlanFileCopies[T any](sources []T, mapFile func(T) (FileCopy, error)) ([]FileCopy, error) {
	items := make([]FileCopy, 0, len(sources))
	for _, source := range sources {
		file, err := mapFile(source)
		if err != nil {
			return nil, err
		}
		items = append(items, file)
	}
	return DeduplicateFileCopies(items)
}

// FileCopyOutcome separates confirmed provider writes from cancellation or
// size or timeout errors observed afterwards. Bytes is negative when a confirmed
// write's size is unavailable; the callback must also return an error.
type FileCopyOutcome struct {
	Confirmed bool
	Bytes     int64
}

// FileCopyReport remains usable on error after all active operations drain.
// CopiedBytes sums validated non-metadata sizes. CopiedPaths includes writes
// racing cancellation, even when size validation fails.
type FileCopyReport struct {
	CopiedPaths []string
	CopiedBytes int64
}

// ExecuteFileCopies executes a validated plan. The adapter owns provider calls,
// timeouts and error context. Progress callbacks are serialized and count
// data bytes only; callers own metadata completion and cleanup policy.
func ExecuteFileCopies(ctx context.Context, items []FileCopy, permits chan struct{},
	copyFile func(context.Context, FileCopy) (FileCopyOutcome, error), completed func(int64),
) (FileCopyReport, error) {
	var report FileCopyReport
	if copyFile == nil {
		return report, merr.WrapErrServiceInternalMsg("copy callback is required")
	}
	var mu sync.Mutex
	err := CopyFiles(ctx, items, permits, func(ctx context.Context, file FileCopy) error {
		outcome, err := copyFile(ctx, file)
		if !outcome.Confirmed {
			if err == nil {
				return merr.WrapErrServiceInternalMsg("copy succeeded without write confirmation")
			}
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		report.CopiedPaths = append(report.CopiedPaths, file.DestinationPath)
		var size int64
		if !file.IsMetadata {
			size = outcome.Bytes
		}
		if size < 0 || size > math.MaxInt64-report.CopiedBytes {
			if err != nil {
				return err
			}
			return merr.WrapErrDataIntegrityMsg("invalid copy size or copy size overflow")
		}
		report.CopiedBytes += size
		if completed != nil {
			completed(size)
		}
		return err
	})
	return report, err
}
