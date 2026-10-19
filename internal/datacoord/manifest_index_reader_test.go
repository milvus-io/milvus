// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestIndexReaderRetriesOnlyRejectedRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	segments := []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{ID: 1}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 2}),
	}
	reader := (&meta{}).newManifestIndexReader(ctx, segments, 2, nil)
	defer reader.close()
	calls := map[int64]int{}
	reader.submitRead = func(_ context.Context, segment *SegmentInfo, complete func([]packed.ManifestIndexInfo, error)) error {
		calls[segment.GetID()]++
		if segment.GetID() == 1 && calls[1] == 1 {
			return merr.ErrServiceUnavailable // Rejection has no callback.
		}
		complete([]packed.ManifestIndexInfo{{BuildID: segment.GetID()}}, nil)
		return nil // A callback may finish before submission returns.
	}
	// A healthy read is delivered while the rejected read waits for retry.
	for _, id := range []int64{2, 1} {
		result, err := reader.next()
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NoError(t, result.err)
		require.Equal(t, id, result.request.segment.GetID())
		require.Equal(t, id, result.entries[0].BuildID)
	}
	result, err := reader.next()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, map[int64]int{1: 2, 2: 1}, calls)
}

func TestManifestIndexReaderCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := (&meta{}).newManifestIndexReader(ctx, []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{ID: 1}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 2}),
	}, 2, nil)
	defer reader.close()
	calls := 0
	reader.submitRead = func(_ context.Context, segment *SegmentInfo, complete func([]packed.ManifestIndexInfo, error)) error {
		calls++
		if segment.GetID() == 1 {
			complete(nil, merr.ErrServiceUnavailable)
		} else {
			complete(nil, nil)
		}
		return nil
	}
	result, err := reader.next()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(2), result.request.segment.GetID())
	cancel()
	result, err = reader.next()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	reader.close() // Discard delayed retries without waiting for a callback.
	require.Equal(t, 2, calls)
}
