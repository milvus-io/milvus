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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// A search that does not declare nq is counted by what it carries, so that
// the rate limiter charges it.
func TestSearchRequestNq(t *testing.T) {
	placeholder := func(rows int) *milvuspb.SearchRequest_PlaceholderGroup {
		values := make([][]byte, rows)
		for i := range values {
			values[i] = []byte{0, 0, 0, 0}
		}
		blob, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{
			{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: values},
		}})
		require.NoError(t, err)
		return &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: blob}
	}
	intIDs := &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}},
	}}

	assert.Equal(t, int64(0), searchRequestNq(&milvuspb.SearchRequest{}))
	assert.Equal(t, int64(3), searchRequestNq(&milvuspb.SearchRequest{SearchInput: placeholder(3)}))
	assert.Equal(t, int64(4), searchRequestNq(&milvuspb.SearchRequest{SearchInput: intIDs}))
	// a declared nq is taken as it is; the search task checks it
	assert.Equal(t, int64(7), searchRequestNq(&milvuspb.SearchRequest{Nq: 7, SearchInput: placeholder(3)}))
	// a malformed group is the search task's to refuse
	assert.Equal(t, int64(0), searchRequestNq(&milvuspb.SearchRequest{
		SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: []byte{0xff}},
	}))

	// The limiter skips a request that costs 0 tokens, so these have to cost
	// their rows.
	cache := NewMockCache(t)
	cache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 100}, nil)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
	originalCache := globalMetaCache
	globalMetaCache = cache
	t.Cleanup(func() { globalMetaCache = originalCache })
	_, _, rt, tokens, err := GetRequestInfo(context.Background(), &milvuspb.SearchRequest{
		DbName: "db", CollectionName: "c", SearchInput: placeholder(3),
	})
	require.NoError(t, err)
	assert.Equal(t, internalpb.RateType_DQLSearch, rt)
	assert.Equal(t, 3, tokens)
}
