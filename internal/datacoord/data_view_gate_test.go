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

package datacoord

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestServer_AliveSegmentMinSchemaVersion(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	im := NewMockImportMeta(t)
	im.EXPECT().GetJobBy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe() // no pending imports
	s := &Server{meta: m, importMeta: im}
	s.stateCode.Store(commonpb.StateCode_Healthy)

	add := func(id, collID int64, state commonpb.SegmentState, level datapb.SegmentLevel, ver int32) {
		require.NoError(t, m.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collID,
			State:         state,
			Level:         level,
			SchemaVersion: ver,
		})))
	}

	// collection 1: min over healthy non-L0 segments is 5.
	add(1, 1, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, 8)
	add(2, 1, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1, 5) // growing counts (its data must still reach the add version)
	add(3, 1, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0, 1) // L0 delete-only, never bumped -> excluded
	add(4, 1, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1, 2) // unhealthy -> excluded

	minVersion, err := s.AliveSegmentMinSchemaVersion(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, int32(5), minVersion)

	// a collection with no alive non-L0 segments -> MaxInt32 (nothing to backfill, gate can release).
	minVersion, err = s.AliveSegmentMinSchemaVersion(context.Background(), 999)
	require.NoError(t, err)
	assert.Equal(t, int32(math.MaxInt32), minVersion)

	// unhealthy server -> error.
	s.stateCode.Store(commonpb.StateCode_Abnormal)
	_, err = s.AliveSegmentMinSchemaVersion(context.Background(), 1)
	assert.Error(t, err)
}

// TestServer_AliveSegmentMinSchemaVersion_PendingImport: an import job created before a schema change is
// bound to an older schema snapshot and has not allocated any segment yet, so the segment scan is empty;
// the fence must fold in the pending job's schema version so the add gate stays held until the import
// completes (else it releases early and the import later materializes a segment missing the added field).
func TestServer_AliveSegmentMinSchemaVersion_PendingImport(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	im := NewMockImportMeta(t)
	s := &Server{meta: m, importMeta: im}
	s.stateCode.Store(commonpb.StateCode_Healthy)

	// no segments yet, but a pending import bound to schema version 3 -> min must be 3, not MaxInt32.
	im.EXPECT().GetJobBy(mock.Anything, mock.Anything, mock.Anything).Return([]ImportJob{
		&importJob{ImportJob: &datapb.ImportJob{
			CollectionID: 1,
			State:        internalpb.ImportJobState_Importing,
			Schema:       &schemapb.CollectionSchema{Version: 3},
		}},
	}).Once()

	minVersion, err := s.AliveSegmentMinSchemaVersion(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, int32(3), minVersion)
}
