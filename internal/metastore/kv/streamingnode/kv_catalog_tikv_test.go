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

package streamingnode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	tikvutil "github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

// A StreamingNode on a TiKV meta store creates the consume checkpoint of a new
// pchannel, publishes components with it, advances it, and refuses an older
// publisher. The first creation once retried until its context ended, because
// TiKV did not implement the version-0 CompareVersionAndSwap it uses.
func TestCatalogCheckpointLifecycleOnTiKV(t *testing.T) {
	cli := tikvutil.SetupLocalTxn()
	defer cli.Close()
	catalog := NewCataLog(tikvkv.NewTiKV(cli, "streamingnode-tikv-test/meta"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const pchannel = "p1"

	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, pchannel, &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 1, Term: 1},
	}))
	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, pchannel)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), checkpoint.GetTimeTick())

	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, pchannel, &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
		},
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 2, Term: 1},
	}))
	segments, err := catalog.ListSegmentAssignment(ctx, pchannel)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	assert.Equal(t, int64(1), segments[0].GetSegmentId())
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, pchannel)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), checkpoint.GetTimeTick())

	// a successor takes the pchannel over with a newer term
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, pchannel, &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 2, Term: 2},
	}))
	err = catalog.SaveRecoverySnapshot(ctx, pchannel, &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 3, Term: 1},
	})
	require.Error(t, err)
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, pchannel)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), checkpoint.GetTimeTick())
	assert.Equal(t, int64(2), checkpoint.GetTerm())
}
