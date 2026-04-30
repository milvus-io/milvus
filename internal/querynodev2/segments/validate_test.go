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

package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newMockSealedSegment(t *testing.T, id, min, max int64, contained []int64) Segment {
	seg := NewMockSegment(t)
	var minPk storage.PrimaryKey = storage.NewInt64PrimaryKey(min)
	var maxPk storage.PrimaryKey = storage.NewInt64PrimaryKey(max)
	containedSet := typeutil.NewSet(contained...)

	seg.EXPECT().ID().Return(id).Maybe()
	seg.EXPECT().PkCandidateExist().Return(true).Maybe()
	seg.EXPECT().GetMinPk().Return(&minPk).Maybe()
	seg.EXPECT().GetMaxPk().Return(&maxPk).Maybe()
	seg.EXPECT().BatchPkExist(mock.AnythingOfType("*storage.BatchLocationsCache")).RunAndReturn(
		func(lc *storage.BatchLocationsCache) []bool {
			hits := make([]bool, 0, lc.Size())
			for _, pk := range lc.PKs() {
				value, ok := pk.GetValue().(int64)
				hits = append(hits, ok && containedSet.Contain(value))
			}
			return hits
		},
	).Maybe()

	return seg
}

func TestValidateOnHistoricalReturnsAllSegments(t *testing.T) {
	paramtable.Init()

	collectionID := int64(101)
	seg1 := newMockSealedSegment(t, 1, 0, 4, []int64{1, 2, 4})
	seg2 := newMockSealedSegment(t, 2, 5, 8, []int64{5, 7})

	collectionManager := NewMockCollectionManager(t)
	collectionManager.EXPECT().Get(collectionID).Return(&Collection{}).Once()

	segmentManager := NewMockSegmentManager(t)
	segmentManager.EXPECT().
		GetAndPin(mock.Anything, mock.Anything).
		Return([]Segment{seg1, seg2}, nil).
		Once()

	manager := &Manager{
		Collection: collectionManager,
		Segment:    segmentManager,
	}

	segments, err := validateOnHistorical(context.Background(), manager, collectionID, nil, []int64{1, 2})
	require.NoError(t, err)
	assert.Same(t, seg1, segments[0])
	assert.Same(t, seg2, segments[1])
}
