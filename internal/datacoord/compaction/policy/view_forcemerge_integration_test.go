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

package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestGroupByPartitionChannel(t *testing.T) {
	label1 := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch1",
	}
	label2 := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  20,
		Channel:      "ch1",
	}

	segments := []*meta.SegmentInfo{
		newForceMergeSegmentForLabel(1, label1),
		newForceMergeSegmentForLabel(2, label1),
		newForceMergeSegmentForLabel(3, label2),
	}

	groups := groupByPartitionChannel(segments)
	assert.Equal(t, 2, len(groups))

	var count1, count2 int
	for _, segs := range groups {
		if len(segs) == 2 {
			count1++
		} else if len(segs) == 1 {
			count2++
		}
	}
	assert.Equal(t, 1, count1)
	assert.Equal(t, 1, count2)
}

func TestGroupByPartitionChannel_EmptySegments(t *testing.T) {
	groups := groupByPartitionChannel([]*meta.SegmentInfo{})
	assert.Empty(t, groups)
}

func TestGroupByPartitionChannel_SameLabel(t *testing.T) {
	label := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch1",
	}

	segments := []*meta.SegmentInfo{
		newForceMergeSegmentForLabel(1, label),
		newForceMergeSegmentForLabel(2, label),
		newForceMergeSegmentForLabel(3, label),
	}

	groups := groupByPartitionChannel(segments)
	assert.Equal(t, 1, len(groups))
	for _, segs := range groups {
		assert.Equal(t, 3, len(segs))
	}
}

func newForceMergeSegmentForLabel(id int64, label *view.CompactionGroupLabel) *meta.SegmentInfo {
	return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            id,
		CollectionID:  label.CollectionID,
		PartitionID:   label.PartitionID,
		InsertChannel: label.Channel,
	}}
}
