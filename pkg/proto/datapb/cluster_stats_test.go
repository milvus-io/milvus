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

package datapb_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSegmentInfoClusterStatsWireFields(t *testing.T) {
	segment := &datapb.SegmentInfo{
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 10, CompactVersion: 20},
		ClusterStats: &datapb.ClusterStats{
			Version: 1, FieldId: 100, GroupId: 2, NumRows: 3,
			CentroidIds: []uint32{4, 5}, Sorted: true,
		},
	}
	fields := segment.ProtoReflect().Descriptor().Fields()
	require.EqualValues(t, 41, fields.ByName("sealed_at_data_version").Number())
	require.EqualValues(t, 42, fields.ByName("cluster_stats").Number())

	encoded, err := proto.Marshal(segment)
	require.NoError(t, err)
	var tags []protowire.Number
	for remaining := encoded; len(remaining) > 0; {
		number, wireType, tagSize := protowire.ConsumeTag(remaining)
		require.Positive(t, tagSize)
		valueSize := protowire.ConsumeFieldValue(number, wireType, remaining[tagSize:])
		require.Positive(t, valueSize)
		tags = append(tags, number)
		remaining = remaining[tagSize+valueSize:]
	}
	require.ElementsMatch(t, []protowire.Number{41, 42}, tags)

	decoded := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(encoded, decoded))
	require.True(t, proto.Equal(segment, decoded))
}
