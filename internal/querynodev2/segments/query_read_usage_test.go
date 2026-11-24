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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/querymeter"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestCollectSealedPredicateReadUsage(t *testing.T) {
	segment := NewMockSegment(t)
	segment.EXPECT().ID().Return(7).Maybe()
	manifest := packed.MarshalManifestPath("segments/7", 10)
	info := &querypb.SegmentLoadInfo{
		ManifestPath:              manifest,
		FieldLogicalSizesManifest: manifest,
		Level:                     datapb.SegmentLevel_L1,
		StorageVersion:            storage.StorageV3,
		NumOfRows:                 8,
		Stats: &datapb.Statistics{FieldLogicalSizes: &datapb.FieldLogicalSizes{
			Version:  storage.FieldLogicalSizeVersion,
			RowCount: 8,
			Fields: map[int64]*datapb.FieldLogicalSize{
				100: {PayloadBytes: 32, ValidityBits: 8},
				101: {},
			},
		}},
	}
	read := &sealedReadState{Segment: segment, fieldSizes: sealedQueryFieldSizes(info)}

	usage, unlock, err := collectPlannedReadUsage(read, []int64{100, 101, 100})
	unlock()
	require.NoError(t, err)
	bytes, err := querymeter.BillableBytes(usage, 0)
	require.NoError(t, err)
	require.EqualValues(t, 66, bytes)

	_, unlock, err = collectPlannedReadUsage(read, []int64{102})
	unlock()
	require.ErrorContains(t, err, "segment 7 field 102")

	info.ManifestPath = packed.MarshalManifestPath("segments/7", 11)
	read.fieldSizes = sealedQueryFieldSizes(info)
	_, unlock, err = collectPlannedReadUsage(read, []int64{100})
	unlock()
	require.ErrorContains(t, err, "sealed manifest")
}
