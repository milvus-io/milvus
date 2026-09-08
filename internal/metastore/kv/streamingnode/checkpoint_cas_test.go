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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// checkpointBytesOf serializes a checkpoint the way SaveRecoverySnapshot
// persists it, to build the exact "current value" of the guarded key.
func checkpointBytesOf(t *testing.T, cp *streamingpb.WALCheckpoint) string {
	t.Helper()
	data, err := proto.Marshal(cp)
	require.NoError(t, err)
	return string(data)
}

// TestSaveRecoverySnapshotCheckpointTermFence proves the term pre-check of
// the consume-checkpoint advancement: a publisher whose term is strictly
// older than the recorded one is refused without touching the store.
func TestSaveRecoverySnapshotCheckpointTermFence(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	key := buildConsumeCheckpointKey("p1")
	current := checkpointBytesOf(t, &streamingpb.WALCheckpoint{TimeTick: 10, Term: 2})
	kv.EXPECT().Load(mock.Anything, key).Return(current, nil)

	err := NewCataLog(kv).SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 20, Term: 1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fenced")
}

// TestSaveRecoverySnapshotCheckpointSameOrNewerTermAdvance proves a publisher
// of the same or a newer term advances the checkpoint through the guarded
// commit, and the post-commit verification confirms the write landed.
func TestSaveRecoverySnapshotCheckpointSameOrNewerTermAdvance(t *testing.T) {
	key := buildConsumeCheckpointKey("p1")
	for _, tc := range []struct {
		name    string
		oldTerm int64
		newTerm int64
	}{
		{name: "same term", oldTerm: 1, newTerm: 1},
		{name: "newer term", oldTerm: 1, newTerm: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := mocks.NewMetaKv(t)
			kv.EXPECT().MaxTxnOps().Return(128)
			current := checkpointBytesOf(t, &streamingpb.WALCheckpoint{TimeTick: 10, Term: tc.oldTerm})
			persisted := &streamingpb.WALCheckpoint{TimeTick: 20, Term: tc.newTerm}
			persistedValue := checkpointBytesOf(t, persisted)
			// First Load reads the current value, the post-commit
			// verification Load reads back the persisted value.
			loads := 0
			kv.EXPECT().Load(mock.Anything, key).
				RunAndReturn(func(_ context.Context, _ string) (string, error) {
					loads++
					if loads == 1 {
						return current, nil
					}
					return persistedValue, nil
				}).Times(2)
			kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.MatchedBy(func(saves map[string]string) bool {
				return saves[key] == persistedValue
			}), mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, _ map[string]string, _ []string, preds ...predicates.Predicate) error {
					require.Len(t, preds, 1)
					assert.Equal(t, key, preds[0].Key())
					assert.Equal(t, current, preds[0].TargetValue())
					return nil
				})
			err := NewCataLog(kv).SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
				ConsumeCheckpoint: persisted,
			})
			assert.NoError(t, err)
		})
	}
}

// TestSaveRecoverySnapshotCheckpointLostCAS proves the post-commit
// verification catches a guarded commit whose guard failed silently: the etcd
// txn reports success even when the value comparison fails, so the read-back
// must be authoritative.
func TestSaveRecoverySnapshotCheckpointLostCAS(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().MaxTxnOps().Return(128)
	key := buildConsumeCheckpointKey("p1")
	current := checkpointBytesOf(t, &streamingpb.WALCheckpoint{TimeTick: 10, Term: 1})
	persistedValue := checkpointBytesOf(t, &streamingpb.WALCheckpoint{TimeTick: 20, Term: 1})
	// A concurrent publisher advanced the checkpoint between our read and the
	// commit; the guarded txn silently does not land, and the read-back shows
	// someone else's value.
	loads := 0
	kv.EXPECT().Load(mock.Anything, key).
		RunAndReturn(func(_ context.Context, _ string) (string, error) {
			loads++
			if loads == 1 {
				return current, nil
			}
			return checkpointBytesOf(t, &streamingpb.WALCheckpoint{TimeTick: 30, Term: 1}), nil
		}).Times(2)
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	_ = persistedValue

	err := NewCataLog(kv).SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 20, Term: 1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "concurrently")
}
