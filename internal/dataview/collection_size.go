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

package dataview

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *dataViewManager) sizeProjector() CollectionSizeProjector {
	if m.collectionSizeEnabled == nil || !m.collectionSizeEnabled() {
		return nil
	}
	return m.collectionSizeProjector
}

// sample runs only for a new DataVersion under its collection lock. Recovery
// and idempotent publications reuse the persisted measurement.
func (project CollectionSizeProjector) sample(ctx context.Context, view *viewpb.DataViewOfCollection, omitData []int64) error {
	view.CollectionSizeBytes = nil
	if project == nil {
		return nil
	}
	size, err := project(ctx, view, omitData)
	if err != nil {
		return err
	}
	view.CollectionSizeBytes = &size
	return nil
}

// PreparedFlush holds the candidate DataView until its SegmentMeta transaction
// has staged the final cumulative logs. Index bytes are already captured, before
// the transaction takes segment writer locks.
type PreparedFlush struct {
	view       *viewpb.DataViewOfCollection
	pending    []int64
	otherBytes int64
}

func (f *PreparedFlush) Version() *viewpb.DataVersion {
	if f == nil {
		return nil
	}
	return f.view.GetDataVersion()
}

// Snapshot completes the candidate with the exact segment data being committed.
// A replay has no pending IDs and returns its original measurement untouched.
func (f *PreparedFlush) Snapshot(segmentBytes func(int64) (int64, error)) (*viewpb.DataViewOfCollection, error) {
	if f == nil {
		return nil, nil
	}
	if len(f.pending) > 0 {
		if segmentBytes == nil {
			return nil, merr.WrapErrServiceInternalMsg("flush requires staged segment sizes")
		}
		size := f.otherBytes
		for _, id := range f.pending {
			n, err := segmentBytes(id)
			if err != nil {
				return nil, err
			}
			size += n
		}
		f.view.CollectionSizeBytes = &size
	}
	return f.view, nil
}
