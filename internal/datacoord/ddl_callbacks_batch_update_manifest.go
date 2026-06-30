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
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (c *DDLCallbacks) batchUpdateManifestV2AckCallback(ctx context.Context, result message.BroadcastResultBatchUpdateManifestMessageV2) error {
	body := result.Message.MustBody()
	// field_backfill is set only when this batch is a bump_defence field backfill; a generic
	// manifest update leaves it nil and takes no gate action. When present, the batch's
	// committed segments must be gated with the per-segment versions the apply below produces.
	fb := body.GetFieldBackfill()
	var (
		operators  []UpdateOperator
		v2Count    int
		v3Count    int
		segVCommit map[int64]int64 // gate thresholds; built only for a backfill batch
		v2Segs     []int64         // v2 segments whose DataVersion is read AFTER the apply
	)
	if fb != nil {
		segVCommit = make(map[int64]int64, len(body.GetItems()))
	}
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
			if fb != nil {
				// v2's committed version (DataVersion) is only assigned by the apply below
				// (DataVersion++), so read it back afterwards.
				v2Segs = append(v2Segs, segID)
			}
		case hasV3:
			operators = append(operators, UpdateManifestVersion(segID, item.GetManifestVersion()))
			v3Count++
			if fb != nil {
				segVCommit[segID] = item.GetManifestVersion()
			}
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
	if fb != nil {
		for _, segID := range v2Segs {
			if seg := c.meta.GetHealthySegment(ctx, segID); seg != nil {
				segVCommit[segID] = int64(seg.GetDataVersion())
			}
		}
		c.registerBackfillGate(ctx, result, fb.GetFieldIds(), segVCommit)
	}
	mlog.Info(ctx, "batch update manifest handled",
		mlog.Int("itemCount", len(body.GetItems())),
		mlog.Int("v3Count", v3Count),
		mlog.Int("v2Count", v2Count))
	return nil
}

// registerBackfillGate registers the bump_defence gate for a field-backfill batch, keyed on
// the broadcast ID so a retried / recovered ack dedupes to a single round. Registration is
// atomic with the durable manifest apply: the gate exists iff this batch applied, and each
// committed segment carries the version it must reach (v3 = committed manifest version,
// v2 = post-apply DataVersion), so the gate revokes exactly when every segment has reopened.
func (c *DDLCallbacks) registerBackfillGate(ctx context.Context, result message.BroadcastResultBatchUpdateManifestMessageV2, fieldIDs []int64, segVCommit map[int64]int64) {
	if c.backfillGate == nil || len(fieldIDs) == 0 || len(segVCommit) == 0 {
		return
	}
	collectionID := result.Message.Header().GetCollectionId()
	source := fmt.Sprintf("batchmanifest:%d", result.Message.BroadcastHeader().BroadcastID)
	roundID, err := c.allocator.AllocID(ctx)
	if err != nil {
		mlog.Warn(ctx, "failed to allocate bump_defence roundID; skip registration",
			mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return
	}
	if err := c.backfillGate.RegisterSegmentList(ctx, collectionID, roundID, source, fieldIDs, segVCommit); err != nil {
		mlog.Warn(ctx, "failed to register bump_defence for external backfill",
			mlog.FieldCollectionID(collectionID), mlog.Err(err))
	}
}
