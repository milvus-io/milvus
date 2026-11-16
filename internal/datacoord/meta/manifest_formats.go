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

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type manifestFormatReader struct {
	once  sync.Once
	io    *packed.ManifestIOContext
	cache sync.Map // segment ID to manifestFormats
}

type manifestFormats struct {
	path    string
	formats []string
}

func (r *manifestFormatReader) executor() *packed.ManifestIOContext {
	r.once.Do(func() {
		r.io = packed.NewManifestIOContext(SegmentIndexManifestReadConcurrency())
	})
	return r.io
}

func (r *manifestFormatReader) close() {
	r.executor().Close()
}

// CachedManifestInsertFormats returns the successful observation of this
// published revision, if available. The manifest pointer includes the revision.
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

// GetManifestInsertFormats uses only data groups from the segment's published
// revision. Successful observations can be shared across targets and rounds.
func (m *Meta) GetManifestInsertFormats(ctx context.Context, segment *SegmentInfo, config *indexpb.StorageConfig) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if formats, found := m.CachedManifestInsertFormats(segment); found {
		return formats, nil
	}
	id := segment.GetID()
	revision, err := packed.ParseManifestPath(segment.GetManifestPath())
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode published manifest revision of segment %d", id)
	}
	if revision.BasePath == "" || revision.ManifestVersion <= packed.ManifestEarliest {
		return nil, merr.WrapErrDataIntegrityMsg("segment %d has no committed manifest revision", segment.GetID())
	}

	reader := &m.manifestFormats
	m.manifestReadOnce.Do(func() { m.manifestReadSlots = newManifestReadSlots() })
	if err := m.manifestReadSlots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer m.manifestReadSlots.Release(1)
	if formats, found := m.CachedManifestInsertFormats(segment); found {
		return formats, nil
	}

	formats, err := packed.ReadManifestInsertFormats(ctx, reader.executor(), revision, config)
	if err != nil {
		return nil, merr.Wrapf(err, "read insert formats of segment %d", id)
	}

	// Serialize installation with publication and GC so a read of an old or
	// removed revision cannot replace the observation of a current segment.
	locks := m.GetSegmentLocks()
	if locks.TryRLock(id) {
		defer locks.RUnlock(id)
		current := m.Segments.GetSegment(id)
		if IsSegmentHealthy(current) && current.GetManifestPath() == segment.GetManifestPath() {
			reader.cache.Store(id, manifestFormats{path: segment.GetManifestPath(), formats: formats})
		}
	}
	return slices.Clone(formats), ctx.Err()
}
