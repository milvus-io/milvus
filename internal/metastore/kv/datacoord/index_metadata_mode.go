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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const indexMetadataModeKey = MetaPrefix + "/index-metadata-mode"

// EnsureIndexMetadataMode persists the first choice before DataCoord admits work.
func (kc *Catalog) EnsureIndexMetadataMode(ctx context.Context, summaries, hasCollections bool) error {
	requested := "legacy"
	if summaries {
		requested = "segment-summary-v1"
	}
	current, err := kc.MetaKv.Load(ctx, indexMetadataModeKey)
	if err == nil {
		if current != requested {
			return merr.Wrap(merr.ErrServiceUnimplemented, "index metadata mode cannot change after cluster initialization")
		}
		return nil
	}
	if !errors.Is(err, merr.ErrIoKeyNotFound) {
		return merr.Wrap(err, "load index metadata mode")
	}
	if summaries {
		if hasCollections {
			return merr.Wrap(merr.ErrServiceUnimplemented, "segment index summaries require a new empty cluster")
		}
		for _, prefix := range []string{SegmentPrefix, util.SegmentIndexPrefix, util.FieldIndexPrefix, SnapshotPrefix, ImportJobPrefix, CopySegmentJobPrefix} {
			exists, err := kc.MetaKv.HasPrefix(ctx, prefix+"/")
			if err != nil {
				return merr.Wrap(err, "check index metadata mode eligibility")
			}
			if exists {
				return merr.Wrap(merr.ErrServiceUnimplemented, "segment index summaries require a new empty cluster")
			}
		}
	}
	swapped, err := kc.MetaKv.CompareVersionAndSwap(ctx, indexMetadataModeKey, 0, requested)
	if err != nil {
		return merr.Wrap(err, "initialize index metadata mode")
	}
	if swapped {
		return nil
	}
	current, err = kc.MetaKv.Load(ctx, indexMetadataModeKey)
	if err != nil {
		return merr.Wrap(err, "reload index metadata mode")
	}
	if current != requested {
		return merr.Wrap(merr.ErrServiceUnimplemented, "index metadata mode was initialized with another mode")
	}
	return nil
}
