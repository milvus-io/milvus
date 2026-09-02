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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// groupedTestRequest builds one real SearchRequest. The fan-out reads
// SearchFieldID and GetNumOfQuery off it, which are cgo calls, so a stub will
// not do -- but no real segment is needed, which keeps this test independent
// of the heavier segment fixtures in this package.
func groupedTestRequest(t *testing.T) (*SearchRequest, *Manager) {
	t.Helper()
	paramtable.Init()

	const collectionID = 100
	schema := mock_segcore.GenTestCollectionSchema("grouped-search", schemapb.DataType_Int64, false)
	manager := NewManager()
	manager.Collection.PutOrRef(collectionID, schema,
		mock_segcore.GenTestIndexMeta(collectionID, schema),
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: collectionID,
			PartitionIDs: []int64{10},
		})
	collection := manager.Collection.Get(collectionID)
	require.NotNil(t, collection)
	t.Cleanup(func() { manager.Collection.Unref(collectionID, 1) })

	req, err := mock_segcore.GenSearchPlanAndRequests(
		collection.GetCCollection(), []int64{1}, mock_segcore.IndexFaissIDMap, 1)
	require.NoError(t, err)
	t.Cleanup(req.Delete)
	return req, manager
}

// groupedMockSegment returns a segment whose SearchGrouped hands back one
// result per branch, tagging each with the (segment, branch) slot that made it
// so the transpose can be checked.
func groupedMockSegment(t *testing.T, segIdx int, tag map[*SearchResult][2]int) Segment {
	t.Helper()
	m := NewMockSegment(t)
	m.EXPECT().DatabaseName().Return("default").Maybe()
	m.EXPECT().ResourceGroup().Return("rg").Maybe()
	m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
	m.EXPECT().SearchGrouped(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, reqs []*SearchRequest) ([]*SearchResult, error) {
			out := make([]*SearchResult, len(reqs))
			for b := range reqs {
				r := new(SearchResult)
				out[b] = r
				tag[r] = [2]int{segIdx, b}
			}
			return out, nil
		}).Once()
	return m
}

// The per-branch reduce consumes one branch's results across all segments, so
// the fan-out must return results[branch][segment] -- not the branch-minor
// order the per-segment call produces.
func TestSearchSegmentsGroupedIsBranchMajor(t *testing.T) {
	const segCount, branches = 3, 2
	req, manager := groupedTestRequest(t)

	tag := make(map[*SearchResult][2]int)
	segs := make([]Segment, 0, segCount)
	for i := 0; i < segCount; i++ {
		segs = append(segs, groupedMockSegment(t, i, tag))
	}
	reqs := make([]*SearchRequest, branches)
	for i := range reqs {
		reqs[i] = req
	}

	got, err := searchSegmentsGroupedAttempt(context.Background(), manager, segs, SegmentTypeSealed, reqs)
	require.NoError(t, err)
	require.Len(t, got, branches)
	for b := range got {
		require.Lenf(t, got[b], segCount, "branch %d", b)
		for s, r := range got[b] {
			require.NotNil(t, r)
			assert.Equalf(t, b, tag[r][1], "branch slot %d holds branch %d", b, tag[r][1])
			assert.Equalf(t, s, tag[r][0], "segment slot %d holds segment %d", s, tag[r][0])
		}
	}
}

// len(reqs) == 1 must reproduce the ungrouped fan-out; that is what makes the
// grouped path a superset rather than a second code path.
func TestSearchSegmentsGroupedSingleBranch(t *testing.T) {
	req, manager := groupedTestRequest(t)
	tag := make(map[*SearchResult][2]int)
	segs := []Segment{groupedMockSegment(t, 0, tag), groupedMockSegment(t, 1, tag)}

	got, err := searchSegmentsGroupedAttempt(context.Background(), manager, segs,
		SegmentTypeSealed, []*SearchRequest{req})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Len(t, got[0], 2)
}

// A segment returning the wrong number of results would silently misalign
// every branch downstream, so it must be rejected rather than tolerated.
func TestSearchSegmentsGroupedRejectsBranchCountMismatch(t *testing.T) {
	req, manager := groupedTestRequest(t)

	m := NewMockSegment(t)
	m.EXPECT().DatabaseName().Return("default").Maybe()
	m.EXPECT().ResourceGroup().Return("rg").Maybe()
	m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
	m.EXPECT().SearchGrouped(mock.Anything, mock.Anything).
		Return([]*SearchResult{new(SearchResult)}, nil).Once()

	_, err := searchSegmentsGroupedAttempt(context.Background(), manager, []Segment{m},
		SegmentTypeSealed, []*SearchRequest{req, req})
	assert.Error(t, err)
}

func TestSearchSegmentsGroupedPropagatesSegmentError(t *testing.T) {
	req, manager := groupedTestRequest(t)

	mkSeg := func(fail bool) Segment {
		m := NewMockSegment(t)
		m.EXPECT().DatabaseName().Return("default").Maybe()
		m.EXPECT().ResourceGroup().Return("rg").Maybe()
		m.EXPECT().ExistIndex(mock.Anything).Return(true).Maybe()
		m.EXPECT().SearchGrouped(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, reqs []*SearchRequest) ([]*SearchResult, error) {
				if fail {
					return nil, errors.New("segment blew up")
				}
				out := make([]*SearchResult, len(reqs))
				for i := range out {
					out[i] = new(SearchResult)
				}
				return out, nil
			}).Maybe()
		return m
	}

	_, err := searchSegmentsGroupedAttempt(context.Background(), manager,
		[]Segment{mkSeg(false), mkSeg(true)}, SegmentTypeSealed, []*SearchRequest{req, req})
	assert.Error(t, err)
}
