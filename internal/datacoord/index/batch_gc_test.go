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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRemoveSegmentIndexesRetainsFailedOrChangedRecords(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	m := NewSegmentIndexMeta(catalog)
	original := &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"a"}}
	m.updateSegmentIndex(original)
	candidate := model.CloneSegmentIndex(original)
	newer := model.CloneSegmentIndex(original)
	newer.IndexFileKeys = []string{"b"}
	m.updateSegmentIndex(newer)
	count, err := m.RemoveSegmentIndexes(t.Context(), []*model.SegmentIndex{candidate})
	require.NoError(t, err)
	require.Zero(t, count)
	catalog.AssertNotCalled(t, "DropSegmentIndexes", mock.Anything, mock.Anything)

	catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).Return(merr.ErrIoTooManyRequests).Once()
	count, err = m.RemoveSegmentIndexes(t.Context(), []*model.SegmentIndex{newer, newer, nil})
	require.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	require.Zero(t, count)
	current, exists := m.SegmentBuildInfo.Get(newer.BuildID)
	require.True(t, exists)
	require.Same(t, newer, current)
	catalog.EXPECT().DropSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, indexes []*model.SegmentIndex) error {
		require.Len(t, indexes, 1)
		return nil
	}).Once()
	count, err = m.RemoveSegmentIndexes(t.Context(), []*model.SegmentIndex{newer, newer, nil})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	_, exists = m.GetIndexJob(newer.BuildID)
	require.False(t, exists)
	count, err = m.RemoveSegmentIndexes(t.Context(), []*model.SegmentIndex{newer})
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestRemoveIndexesRetainsLiveOrChangedDefinitions(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	m := NewSegmentIndexMeta(catalog)
	deleted := &model.Index{CollectionID: 1, IndexID: 1, IsDeleted: true}
	live := &model.Index{CollectionID: 1, IndexID: 2}
	m.Indexes[1] = map[int64]*model.Index{1: deleted, 2: live}
	stale := model.CloneIndex(deleted)
	count, err := m.RemoveIndexes(t.Context(), []*model.Index{live, stale})
	require.NoError(t, err)
	require.Zero(t, count)
	catalog.AssertNotCalled(t, "DropIndexes", mock.Anything, mock.Anything)
	catalog.EXPECT().DropIndexes(mock.Anything, mock.Anything).Return(merr.ErrIoTooManyRequests).Once()
	count, err = m.RemoveIndexes(t.Context(), []*model.Index{deleted})
	require.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	require.Zero(t, count)
	require.Same(t, deleted, m.Indexes[1][1])
	catalog.EXPECT().DropIndexes(mock.Anything, mock.Anything).Return(nil).Once()
	count, err = m.RemoveIndexes(t.Context(), []*model.Index{deleted, nil, deleted})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NotContains(t, m.Indexes[1], int64(1))
	require.Same(t, live, m.Indexes[1][2])
}
