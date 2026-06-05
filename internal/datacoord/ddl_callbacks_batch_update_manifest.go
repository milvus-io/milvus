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

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (c *DDLCallbacks) batchUpdateManifestV2AckCallback(ctx context.Context, result message.BroadcastResultBatchUpdateManifestMessageV2) error {
	body := result.Message.MustBody()
	var (
		mutations  = make(map[int64][]MutateFunc, len(body.GetItems()))
		segmentIDs []UniqueID
		v2Count    int
		v3Count    int
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
			groups := cg.GetColumnGroups()
			mutations[segID] = append(mutations[segID], func(seg *datapb.SegmentInfo) bool {
				incomingChildFields := make(map[int64]struct{})
				for _, group := range groups {
					for _, childField := range group.GetChildFields() {
						incomingChildFields[childField] = struct{}{}
					}
				}

				kept := seg.Binlogs[:0]
				for _, existing := range seg.GetBinlogs() {
					if _, replaced := groups[existing.GetFieldID()]; replaced {
						continue
					}
					if len(existing.GetChildFields()) > 0 {
						childFields := existing.ChildFields[:0]
						for _, childField := range existing.GetChildFields() {
							if _, incoming := incomingChildFields[childField]; !incoming {
								childFields = append(childFields, childField)
							}
						}
						existing.ChildFields = childFields
						if len(existing.ChildFields) == 0 {
							continue
						}
					}
					kept = append(kept, existing)
				}
				seg.Binlogs = kept
				for _, group := range groups {
					seg.Binlogs = append(seg.Binlogs, group)
				}
				seg.DataVersion++
				return true
			})
			segmentIDs = append(segmentIDs, segID)
			v2Count++
		case hasV3:
			manifestVersion := item.GetManifestVersion()
			mutations[segID] = append(mutations[segID], func(seg *datapb.SegmentInfo) bool {
				if seg.GetManifestPath() == "" {
					mlog.Warn(ctx, "batch update manifest version skipped: no manifest path",
						mlog.FieldSegmentID(segID))
					return false
				}
				basePath, currentVer, err := packed.UnmarshalManifestPath(seg.GetManifestPath())
				if err != nil {
					mlog.Warn(ctx, "batch update manifest version skipped: invalid manifest path",
						mlog.FieldSegmentID(segID), mlog.Err(err))
					return false
				}
				if currentVer >= manifestVersion {
					if currentVer > manifestVersion {
						mlog.Warn(ctx, "batch update manifest version skipped: version regression",
							mlog.FieldSegmentID(segID),
							mlog.Int64("currentVer", currentVer),
							mlog.Int64("incomingVer", manifestVersion))
					}
					return false
				}
				seg.ManifestPath = packed.MarshalManifestPath(basePath, manifestVersion)
				return true
			})
			segmentIDs = append(segmentIDs, segID)
			v3Count++
		default:
			mlog.Warn(ctx, "batch update manifest item has no payload; skipping",
				mlog.FieldSegmentID(segID))
		}
	}
	if len(mutations) > 0 {
		if err := c.meta.UpdateSegmentsInfo(ctx, mutations); err != nil {
			mlog.Warn(ctx, "batch update manifest failed", mlog.Err(err))
			return err
		}
		notifySegmentIndexBuild(segmentIDs...)
	}
	mlog.Info(ctx, "batch update manifest handled",
		mlog.Int("itemCount", len(body.GetItems())),
		mlog.Int("v3Count", v3Count),
		mlog.Int("v2Count", v2Count))
	return nil
}
