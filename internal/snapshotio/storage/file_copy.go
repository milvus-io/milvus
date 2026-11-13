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

package storage

import (
	"context"

	"golang.org/x/sync/errgroup"

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
	if cap(permits) <= 0 || copyFile == nil {
		return merr.WrapErrServiceInternalMsg("invalid file copy executor configuration")
	}
	group, copyCtx := errgroup.WithContext(ctx)
	// Acquire before launching: concurrent copy plans must not each create
	// a full task-sized pool of goroutines waiting for the same permits.
copyLoop:
	for _, file := range items {
		select {
		case permits <- struct{}{}:
		case <-copyCtx.Done():
			break copyLoop
		}
		if copyCtx.Err() != nil {
			<-permits
			break
		}
		group.Go(func() error {
			defer func() { <-permits }()
			return copyFile(copyCtx, file)
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}
