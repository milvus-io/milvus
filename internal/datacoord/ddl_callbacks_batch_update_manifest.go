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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// field_backfill is set only when this batch is a bump_defence field backfill; a
	// generic manifest update leaves it nil and takes no gate action. A registration
	// failure is returned into the ack retry loop, exactly like the apply failure above:
	// the ack completes (and the message is tombstoned) only once BOTH the manifest apply
	// and the gate registration have landed -- otherwise a transient etcd/alloc failure
	// would permanently leave the backfilled fields ungated.
	if fb := body.GetFieldBackfill(); fb != nil {
		if err := c.registerBackfillGate(ctx, result, fb.GetFieldIds(), fb.GetSource()); err != nil {
			mlog.Warn(ctx, "batch update manifest gate registration failed", mlog.Err(err))
			return err
		}
	}
	mlog.Info(ctx, "batch update manifest handled",
		mlog.Int("itemCount", len(body.GetItems())),
		mlog.Int("v3Count", v3Count),
		mlog.Int("v2Count", v2Count))
	return nil
}

// registerBackfillGate registers the bump_defence gate for a field-backfill batch, keyed
// on the COMMIT-level source carried by the message, so every batch of one commit (and
// any retried / recovered ack) dedupes onto a single round. It runs AFTER the manifest
// apply above, so the write side holds by construction: each batch's re-registration
// resets the round's T_c, and the gate revokes once a target built after the LAST
// batch's observation has been promoted (the promote barrier certifies every copy
// loaded + reopened). Errors propagate into the ack retry loop, making registration
// atomic with the durable apply.
func (c *DDLCallbacks) registerBackfillGate(ctx context.Context, result message.BroadcastResultBatchUpdateManifestMessageV2, fieldIDs []int64, source string) error {
	if c.backfillGate == nil || len(fieldIDs) == 0 {
		return nil
	}
	collectionID := result.Message.Header().GetCollectionId()
	if source == "" {
		// Defensive fallback for a message produced without the commit-level source
		// (older build): per-batch identity still dedupes ack retries of that batch.
		source = fmt.Sprintf("batchmanifest:%d", result.Message.BroadcastHeader().BroadcastID)
	}
	roundID, err := c.allocator.AllocID(ctx)
	if err != nil {
		return merr.Wrap(err, "allocate bump_defence roundID")
	}
	if err := c.backfillGate.RegisterExternal(ctx, collectionID, roundID, source, fieldIDs); err != nil {
		return merr.Wrap(err, "register bump_defence gate")
	}
	return nil
}
