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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
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
			log.Ctx(ctx).Warn("batch update manifest item has both V2 and V3 payload; skipping",
				zap.Int64("segmentID", segID))
			continue
		case hasV2:
			operators = append(operators, UpdateSegmentColumnGroupsOperator(segID, cg.GetColumnGroups()))
			v2Count++
		case hasV3:
			operators = append(operators, UpdateManifestVersion(segID, item.GetManifestVersion()))
			v3Count++
		default:
			log.Ctx(ctx).Warn("batch update manifest item has no payload; skipping",
				zap.Int64("segmentID", segID))
		}
	}
	if len(operators) > 0 {
		if err := c.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
			log.Ctx(ctx).Warn("batch update manifest failed", zap.Error(err))
			return err
		}
	}
	log.Ctx(ctx).Info("batch update manifest handled",
		zap.Int("itemCount", len(body.GetItems())),
		zap.Int("v3Count", v3Count),
		zap.Int("v2Count", v2Count))
	return nil
}
