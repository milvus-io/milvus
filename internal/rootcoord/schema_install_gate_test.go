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

package rootcoord

import (
	"context"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/schemaevolution"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type recordingSchemaInstallGate struct {
	prepareErr  error
	completeErr error
	prepared    []int64
	completed   []int64
	aborted     []int64
}

func (gate *recordingSchemaInstallGate) PrepareSchemaInstall(_ context.Context, collectionID int64) error {
	gate.prepared = append(gate.prepared, collectionID)
	return gate.prepareErr
}

func (gate *recordingSchemaInstallGate) CompleteSchemaInstall(_ context.Context, collectionID int64, _ *schemapb.CollectionSchema, _ uint64) error {
	gate.completed = append(gate.completed, collectionID)
	return gate.completeErr
}

func (gate *recordingSchemaInstallGate) AbortSchemaInstall(_ context.Context, collectionID int64) {
	gate.aborted = append(gate.aborted, collectionID)
}

type recordingBroadcastAPI struct {
	err   error
	calls int
}

type schemaInstallVersionProvider struct {
	version semver.Version
}

func (p schemaInstallVersionProvider) GetSessions(_ context.Context, role string) (map[string]*sessionutil.Session, int64, error) {
	return map[string]*sessionutil.Session{
		role: {
			SessionRaw: sessionutil.SessionRaw{ServerID: 1, Version: p.version.String()},
			Version:    p.version,
		},
	}, 1, nil
}

func newSchemaInstallGateTestCore(gate schemaevolution.InstallGate) *Core {
	return &Core{
		schemaInstallGate:            gate,
		schemaInstallVersionProvider: schemaInstallVersionProvider{version: semver.MustParse("3.0.1")},
	}
}

func (api *recordingBroadcastAPI) Broadcast(context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	api.calls++
	return nil, api.err
}

func (api *recordingBroadcastAPI) Close() {}

func newSchemaChangeBroadcastForGateTest(collectionID int64) message.BroadcastMutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema},
			},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: &schemapb.CollectionSchema{Version: 2},
			},
		}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
}

func TestSchemaInstallBroadcasterPrepareFailureAbortsPreCut(t *testing.T) {
	gate := &recordingSchemaInstallGate{prepareErr: context.DeadlineExceeded}
	api := &recordingBroadcastAPI{}
	wrapped := newSchemaInstallGateTestCore(gate).wrapSchemaInstallBroadcaster(api)

	_, err := wrapped.Broadcast(context.Background(), newSchemaChangeBroadcastForGateTest(100))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, []int64{100}, gate.prepared)
	assert.Equal(t, []int64{100}, gate.aborted)
	assert.Zero(t, api.calls)
}

func TestSchemaInstallBroadcasterRejectsOldNodeBeforeClosingGate(t *testing.T) {
	gate := &recordingSchemaInstallGate{}
	api := &recordingBroadcastAPI{}
	core := newSchemaInstallGateTestCore(gate)
	core.schemaInstallVersionProvider = schemaInstallVersionProvider{version: semver.MustParse("3.0.0")}
	wrapper := core.wrapSchemaInstallBroadcaster(api)

	_, err := wrapper.Broadcast(context.Background(), newSchemaChangeBroadcastForGateTest(100))
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	assert.Empty(t, gate.prepared)
	assert.Empty(t, gate.aborted)
	assert.Zero(t, api.calls)
}

func TestSchemaInstallBroadcasterBroadcastFailureCutClassification(t *testing.T) {
	t.Run("task_not_created_aborts", func(t *testing.T) {
		gate := &recordingSchemaInstallGate{}
		api := &recordingBroadcastAPI{err: broadcaster.ErrBroadcastTaskNotCreated}
		wrapped := newSchemaInstallGateTestCore(gate).wrapSchemaInstallBroadcaster(api)

		_, err := wrapped.Broadcast(context.Background(), newSchemaChangeBroadcastForGateTest(100))
		require.ErrorIs(t, err, broadcaster.ErrBroadcastTaskNotCreated)
		assert.Equal(t, []int64{100}, gate.aborted)
	})

	t.Run("ambiguous_failure_keeps_gate_closed", func(t *testing.T) {
		gate := &recordingSchemaInstallGate{}
		api := &recordingBroadcastAPI{err: context.DeadlineExceeded}
		wrapped := newSchemaInstallGateTestCore(gate).wrapSchemaInstallBroadcaster(api)

		_, err := wrapped.Broadcast(context.Background(), newSchemaChangeBroadcastForGateTest(100))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Empty(t, gate.aborted)
	})
}

func TestSchemaInstallAckCollectionNotFoundStillCompletesGate(t *testing.T) {
	collectionID := int64(100)
	gate := &recordingSchemaInstallGate{}
	meta := &mockMetaTable{
		AlterCollectionFunc: func(context.Context, message.BroadcastResultAlterCollectionMessageV2) error {
			return errAlterCollectionNotFound
		},
	}
	core := newTestCore(withMeta(meta))
	core.schemaInstallGate = gate
	callback := &DDLCallback{Core: core}
	msg := message.MustAsBroadcastAlterCollectionMessageV2(newSchemaChangeBroadcastForGateTest(collectionID))

	err := callback.alterCollectionV2AckCallback(context.Background(), message.BroadcastResultAlterCollectionMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{
			"v1": {TimeTick: 200},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, []int64{collectionID}, gate.prepared)
	assert.Equal(t, []int64{collectionID}, gate.completed)
}

func TestSchemaInstallAckFailureKeepsGatePending(t *testing.T) {
	collectionID := int64(100)
	gate := &recordingSchemaInstallGate{}
	meta := &mockMetaTable{
		AlterCollectionFunc: func(context.Context, message.BroadcastResultAlterCollectionMessageV2) error {
			return nil
		},
	}
	core := newTestCore(
		withMeta(meta),
		withBroker(&mockBroker{
			BroadcastAlteredCollectionFunc: func(context.Context, UniqueID) error {
				return context.DeadlineExceeded
			},
		}),
	)
	core.schemaInstallGate = gate
	callback := &DDLCallback{Core: core}
	msg := message.MustAsBroadcastAlterCollectionMessageV2(newSchemaChangeBroadcastForGateTest(collectionID))

	err := callback.alterCollectionV2AckCallback(context.Background(), message.BroadcastResultAlterCollectionMessageV2{
		Message: msg,
		Results: map[string]*message.AppendResult{
			"v1": {TimeTick: 200},
		},
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, []int64{collectionID}, gate.prepared)
	assert.Empty(t, gate.completed)
	assert.Empty(t, gate.aborted)
}
