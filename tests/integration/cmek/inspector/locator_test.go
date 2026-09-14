// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inspector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLocateScalarIndexMatchesDataCoordIndexType(t *testing.T) {
	const (
		collectionID int64 = 10
		segmentID    int64 = 20
		fieldID      int64 = 30
		indexType          = "STL_SORT"
	)

	client := mocks.NewMockMixCoordClient(t)
	client.EXPECT().GetIndexInfos(mock.Anything, mock.MatchedBy(func(req *indexpb.GetIndexInfoRequest) bool {
		return req.GetCollectionID() == collectionID &&
			len(req.GetSegmentIDs()) == 1 && req.GetSegmentIDs()[0] == segmentID &&
			req.GetIndexName() == ""
	})).Return(&indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
		SegmentInfo: map[int64]*indexpb.SegmentInfo{
			segmentID: {
				CollectionID: collectionID,
				SegmentID:    segmentID,
				IndexInfos: []*indexpb.IndexFilePathInfo{
					{
						FieldID:                   fieldID,
						IndexName:                 indexType,
						CurrentScalarIndexVersion: 2,
						IndexFilePaths:            []string{"scalar-index"},
					},
				},
			},
		},
	}, nil)

	objects, err := LocateScalarIndex(context.Background(), client, []*datapb.SegmentInfo{
		{ID: segmentID, CollectionID: collectionID},
	}, indexType, fieldID, 2)
	require.NoError(t, err)
	require.Equal(t, []Object{{
		SegmentID:     segmentID,
		FieldID:       fieldID,
		Path:          "scalar-index",
		EngineVersion: 2,
	}}, objects)
}
