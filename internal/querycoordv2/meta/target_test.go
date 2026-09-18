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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCollectionTarget_IDSetBased(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	store.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10,
		InsertChannel: "ch-0", NumOfRows: 500,
		State: commonpb.SegmentState_Flushed,
	})
	store.PutSegment(&datapb.SegmentInfo{
		ID: 2, CollectionID: 100, PartitionID: 10,
		InsertChannel: "ch-1", NumOfRows: 300,
		State: commonpb.SegmentState_Flushed,
	})

	segIDs := map[int64]struct{}{1: {}, 2: {}}
	dmChannels := map[string]*DmChannel{
		"ch-0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-0", CollectionID: 100}},
		"ch-1": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-1", CollectionID: 100}},
	}

	target := NewCollectionTarget(segIDs, dmChannels, []int64{10}, store)

	allSegs := target.GetAllSegments()
	assert.Len(t, allSegs, 2)
	assert.Equal(t, int64(500), allSegs[1].GetNumOfRows())

	chSegs := target.GetChannelSegments("ch-0")
	assert.Len(t, chSegs, 1)

	assert.Equal(t, int64(800), target.GetRowCount())
}
