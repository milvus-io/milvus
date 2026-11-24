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

package metastore

import (
	"reflect"
	"strconv"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

// SegmentGroupType is the model type of segment change groups, values of
// *model.SegmentChangeGroup at
// datacoord-meta/segment-change-group/{collectionID}/{groupID}, unchanged
// from the typed catalog methods. List with prefix {collectionID} for one
// collection's groups.
var SegmentGroupType = RegisterModelType(ModelType{
	Root:   "datacoord-meta/segment-change-group",
	Depth:  2,
	GoType: reflect.TypeOf((*model.SegmentChangeGroup)(nil)),
	Coordinate: func(v any) []string {
		g := v.(*model.SegmentChangeGroup)
		return []string{strconv.FormatInt(g.CollectionID, 10), strconv.FormatInt(g.GroupID, 10)}
	},
	Marshal: func(v any) ([]byte, error) {
		return model.MarshalSegmentChangeGroup(v.(*model.SegmentChangeGroup))
	},
	// Unmarshal validates the record and fails closed: a group is the only
	// owner of its staged members' visibility (SegmentInfo has no
	// change_group_id field), so a corrupt record must stop recovery instead
	// of being skipped.
	Unmarshal: func(value []byte) (any, error) {
		return model.UnmarshalSegmentChangeGroup(value)
	},
})

// SaveSegmentChangeGroup returns an UpdateAction that persists g as a segment
// change group upsert (full-value replace). It is the write used both for a
// standalone state transition and for composing into a batch write. Order
// is the caller's (C13): write a TERMINAL group (COMMITTED/FAILED/ABORTED)
// in an Update after the one holding its segment actions, and compose an
// ALIVE group (STAGED/READY) BEFORE its staged members, so a crash can never
// leave invisible members with no group record (unrecoverable without
// SegmentInfo.change_group_id).
func SaveSegmentChangeGroup(g *model.SegmentChangeGroup) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: g}
}

// DeleteSegmentChangeGroup returns an UpdateAction that removes a segment
// change group record. Compose it after the group's members and superseded
// segments are fully retired.
func DeleteSegmentChangeGroup(collectionID, groupID int64) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: &model.SegmentChangeGroup{CollectionID: collectionID, GroupID: groupID}}
}
