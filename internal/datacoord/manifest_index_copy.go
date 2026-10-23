// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"path"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func migrationIndexPaths(root, manifest string, record *model.SegmentIndex) []string {
	return metautil.NewIndexPathBuilder(root, record.IndexStorePathVersion,
		record.CollectionID, record.PartitionID, record.SegmentID, record.BuildID,
		record.IndexVersion).WithManifestPath(manifest).BuildFilePaths(record.IndexFileKeys)
}

// copyMigratedIndexFiles runs only after source and destination validation, with
// the segment and selected BuildID locks held. Metadata remains at the source
// until all copies complete. No recursive listing, downloads or uploads are needed.
func (m *meta) copyMigratedIndexFiles(ctx context.Context, segment *SegmentInfo, staged []*stagedSegmentIndexMutation) error {
	type fileCopy struct{ source, destination string }
	var files []fileCopy
	for _, index := range staged {
		if index.source == nil {
			continue
		}
		if index.source.IndexStorePathVersion < indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED ||
			index.source.IndexStorePathVersion > indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED {
			return merr.Wrapf(merr.ErrDataIntegrity, "unknown source index layout for build %d", index.source.BuildID)
		}
		if index.source.IndexStorePathVersion == index.record.IndexStorePathVersion {
			continue
		}
		for _, key := range index.source.IndexFileKeys {
			if key == "" || key == "." || key == ".." || path.IsAbs(key) || path.Base(key) != key {
				return merr.Wrapf(merr.ErrDataIntegrity, "invalid migrating index file key for build %d", index.source.BuildID)
			}
		}
		sources := migrationIndexPaths(m.chunkManager.RootPath(), segment.GetManifestPath(), index.source)
		targets := migrationIndexPaths(m.chunkManager.RootPath(), segment.GetManifestPath(), index.record)
		for idx, source := range sources {
			files = append(files, fileCopy{source, targets[idx]})
		}
	}
	if len(files) == 0 {
		return ctx.Err()
	}
	if _, local := m.chunkManager.(*storage.LocalChunkManager); local {
		return merr.Wrap(merr.ErrServiceUnimplemented, "index artifact migration requires object storage server-side copy")
	}
	m.manifestCopyOnce.Do(func() {
		// A process-wide budget, shared by both inspectors and every segment.
		// Reuse the existing storage I/O concurrency and connection limits.
		m.manifestCopyLimit = segmentIndexManifestReadConcurrency()
		m.manifestCopySlots = semaphore.NewWeighted(int64(m.manifestCopyLimit))
	})
	workers, copyCtx := errgroup.WithContext(ctx)
	workers.SetLimit(min(len(files), m.manifestCopyLimit))
	for _, file := range files {
		if copyCtx.Err() != nil {
			break
		}
		workers.Go(func() error {
			if err := m.manifestCopySlots.Acquire(copyCtx, 1); err != nil {
				return err
			}
			defer m.manifestCopySlots.Release(1)
			if err := copyCtx.Err(); err != nil {
				return err
			}
			if err := m.chunkManager.Copy(copyCtx, file.source, file.destination); err != nil {
				return merr.Wrapf(err, "copy migrating index artifact %s to %s", file.source, file.destination)
			}
			return nil
		})
	}
	// Cancellation never lets publication or shutdown overtake accepted copies.
	if err := workers.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}
