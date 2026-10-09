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

package qnview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSegmentLoadUsesItsOwnTransformStart(t *testing.T) {
	req := AcquirePhysicalSegments{
		Meta: &viewpb.QueryViewMeta{TransformStartAfterTimetick: 40},
		View: &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{SegmentIds: []int64{10, 20}, SegmentTransformStartAfterTimeticks: []uint64{50, 70}}}},
	}
	require.Equal(t, uint64(50), newSegmentLoadRequest(req, 10).transformStartAfterTimeTick)
	require.Equal(t, uint64(70), newSegmentLoadRequest(req, 20).transformStartAfterTimeTick)
	req.View.Partitions[0].SegmentTransformStartAfterTimeticks = nil
	require.Equal(t, uint64(40), newSegmentLoadRequest(req, 10).transformStartAfterTimeTick, "legacy views replay the conservative whole-shard suffix")
}
