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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A failed read can mean absent, corrupt, or temporarily unavailable. Only a
// definitively absent Dropped manifest permits startup to omit its records.
type recoveryManifestExistence struct {
	storage.ChunkManager
	exists bool
	err    error
	calls  int
}

func (c *recoveryManifestExistence) Exist(context.Context, string) (bool, error) {
	c.calls++
	return c.exists, c.err
}

func TestManifestDroppedRecoveryReadFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		state        commonpb.SegmentState
		exists       bool
		existenceErr error
		wantErr      error
		wantProbe    bool
	}{
		{"dropped_absent", commonpb.SegmentState_Dropped, false, nil, nil, true},
		{"dropped_present_transient_read", commonpb.SegmentState_Dropped, true, nil, merr.ErrIoFailed, true},
		{"dropped_existence_error", commonpb.SegmentState_Dropped, false, merr.ErrIoTooManyRequests, merr.ErrIoTooManyRequests, true},
		{"healthy_absent", commonpb.SegmentState_Flushed, false, nil, merr.ErrIoFailed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, mDomains := setupManifestReloadMeta(t)
			segment := m.GetSegment(context.Background(), 5001).Clone()
			segment.State = tc.state
			m.segments.SetSegment(segment.GetID(), segment)
			cm := &recoveryManifestExistence{ChunkManager: m.chunkManager, exists: tc.exists, err: tc.existenceErr}
			m.chunkManager = cm
			reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.WrapErrIoFailedReason("manifest read unavailable")).Build()
			defer reader.UnPatch()
			err := m.reloadSegmentIndexesFromManifests(context.Background())
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.wantProbe, cm.calls > 0)
			require.Empty(t, mDomains.indexMeta.GetAllSegIndexes())
			require.True(t, m.GetSegment(context.Background(), 5001).GetManifestHasIndex(), "absence is not proof of an empty manifest section")
		})
	}
}

func TestManifestDroppedRecoveryRejectsInvalidEntry(t *testing.T) {
	m, _ := setupManifestReloadMeta(t)
	segment := m.GetSegment(context.Background(), 5001).Clone()
	segment.State = commonpb.SegmentState_Dropped
	m.segments.SetSegment(segment.GetID(), segment)
	cm := &recoveryManifestExistence{ChunkManager: m.chunkManager}
	m.chunkManager = cm
	reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{BuildID: 5100}}, nil).Build()
	defer reader.UnPatch()
	require.ErrorIs(t, m.reloadSegmentIndexesFromManifests(context.Background()), merr.ErrDataIntegrity)
	require.Zero(t, cm.calls, "a decoded invalid entry must not use the missing-manifest exception")
}
