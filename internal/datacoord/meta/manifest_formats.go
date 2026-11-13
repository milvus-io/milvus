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

package meta

import (
	"context"
	"slices"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type manifestFormatReader struct {
	once  sync.Once
	slots *semaphore.Weighted
	cache sync.Map // segment ID to manifestFormats
}

type manifestFormats struct {
	path    string
	formats []string
}

// ManifestFormatReadConcurrency limits concurrent synchronous native reads
// on the 3.0 storage library and respects the storage connection budget.
func ManifestFormatReadConcurrency() int {
	const parallelReads = 8
	connections := paramtable.Get().MinioCfg.MaxConnections.GetAsInt()
	if connections > 0 {
		return min(parallelReads, connections)
	}
	return parallelReads
}

// CachedManifestInsertFormats returns a successful observation of this
// published revision, if available.
func (m *Meta) CachedManifestInsertFormats(segment *SegmentInfo) ([]string, bool) {
	value, ok := m.manifestFormats.cache.Load(segment.GetID())
	if !ok {
		return nil, false
	}
	cached := value.(manifestFormats)
	if cached.path != segment.GetManifestPath() {
		return nil, false
	}
	return slices.Clone(cached.formats), true
}

// GetManifestInsertFormats shares observations of published revisions across
// queries and reconciliation rounds.
func (m *Meta) GetManifestInsertFormats(ctx context.Context, segment *SegmentInfo, config *indexpb.StorageConfig) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if formats, found := m.CachedManifestInsertFormats(segment); found {
		return formats, nil
	}
	reader := &m.manifestFormats
	reader.once.Do(func() {
		reader.slots = semaphore.NewWeighted(int64(ManifestFormatReadConcurrency()))
	})
	if err := reader.slots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer reader.slots.Release(1)
	if formats, found := m.CachedManifestInsertFormats(segment); found {
		return formats, nil
	}
	id := segment.GetID()
	revision := segment.GetManifestPath()
	formats, err := packed.ReadManifestInsertFormats(ctx, revision, config)
	if err != nil {
		return nil, merr.Wrapf(err, "read insert formats of segment %d", id)
	}

	// Publication and GC take this lock for writes. Cache only a revision
	// that is still current, and let a contended read retry next time.
	if m.SegMu.TryRLock() {
		defer m.SegMu.RUnlock()
		current := m.Segments.GetSegment(id)
		if IsSegmentHealthy(current) && current.GetManifestPath() == revision {
			reader.cache.Store(id, manifestFormats{path: revision, formats: formats})
		}
	}
	return slices.Clone(formats), ctx.Err()
}
