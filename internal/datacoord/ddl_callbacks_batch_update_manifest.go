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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (c *DDLCallbacks) batchUpdateManifestV2AckCallback(ctx context.Context, result message.BroadcastResultBatchUpdateManifestMessageV2) error {
	body := result.Message.MustBody()
	var (
		operators []UpdateOperator
		v2Count   int
		v3Count   int
	)
	for _, item := range body.GetItems() {
		segID := item.GetSegmentId()
		cg := item.GetV2ColumnGroups()
		hasV3 := item.GetManifestVersion() > 0
		hasV2 := cg != nil && len(cg.GetColumnGroups()) > 0
		switch {
		case hasV2 && hasV3:
			mlog.Warn(ctx, "batch update manifest item has both V2 and V3 payload; skipping",
				mlog.FieldSegmentID(segID))
			continue
		case hasV2:
			operators = append(operators, UpdateSegmentColumnGroupsOperator(segID, cg.GetColumnGroups()))
			v2Count++
		case hasV3:
			// TODO(segment-manifest-commit): a batch broadcast carries up to 512
			// items and this V3 payload is a pure manifest-version bump (no
			// object-storage I/O). We deliberately keep it as an UpdateManifestVersion
			// operator so the whole batch — V2 column groups and V3 version bumps —
			// commits in a single atomic UpdateSegmentsInfo (one AlterSegments).
			//
			// Routing each item through meta.CommitSegmentManifest instead would take
			// the per-segment manifest lock plus segMu twice per item and issue one
			// catalog.Update per item, and a mid-loop failure would return before the
			// accumulated V2 operators are applied — i.e. the batch would no longer be
			// applied as a unit. The open tension is that CommitSegmentManifest's
			// per-segment serialization is what protects against concurrent writers
			// (stats/index/GC/compaction) racing the manifest pointer; skipping it here
			// trades that protection for batch atomicity. This is the same unresolved
			// conflict as the external collection refresh path. Revisit with a batched
			// CommitSegmentManifests primitive that preserves the single-writer
			// invariant without giving up single-transaction batching.
			operators = append(operators, UpdateManifestVersion(segID, item.GetManifestVersion()))
			v3Count++
		default:
			mlog.Warn(ctx, "batch update manifest item has no payload; skipping",
				mlog.FieldSegmentID(segID))
		}
	}
	if len(operators) > 0 {
		if err := c.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
			mlog.Warn(ctx, "batch update manifest failed", mlog.Err(err))
			return err
		}
	}
	mlog.Info(ctx, "batch update manifest handled",
		mlog.Int("itemCount", len(body.GetItems())),
		mlog.Int("v3Count", v3Count),
		mlog.Int("v2Count", v2Count))
	return nil
}
