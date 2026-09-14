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

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShowSegmentsWithContextUsesParentContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var received context.Context

	segments, err := showSegmentsWithContext(
		parent,
		"db",
		"collection",
		func(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
			received = ctx
			require.Equal(t, "db", req.GetDbName())
			require.Equal(t, []string{"collection"}, req.GetCollectionNames())
			return &milvuspb.ShowCollectionsResponse{
				Status:        merr.Success(),
				CollectionIds: []int64{10},
			}, nil
		},
		func(collectionID int64) ([]*datapb.SegmentInfo, error) {
			require.Equal(t, int64(10), collectionID)
			return []*datapb.SegmentInfo{{ID: 20}}, nil
		},
	)

	require.NoError(t, err)
	require.Same(t, parent, received)
	require.Equal(t, int64(20), segments[0].GetID())
}
