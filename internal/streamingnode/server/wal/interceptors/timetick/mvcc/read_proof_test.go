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

package mvcc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestReadProofUsesCompletePublishedPrefix(t *testing.T) {
	cm := NewMVCCManager(10)
	cm.ApplyRecoveryBarrier("v", 10)
	msg := func(tt uint64, typ message.MessageType, txn bool) message.ImmutableMessage {
		return createQueryTestMessage(t, tt, "v", typ, txn, true).IntoImmutableMessage(nil)
	}
	tick := func(tt uint64) message.ImmutableMessage {
		return createQueryTestMessage(t, tt, "", message.MessageTypeTimeTick, false, false).IntoImmutableMessage(nil)
	}
	// An acknowledged later append must not contaminate the certified prefix.
	cm.UpdateMVCC(createQueryTestMessage(t, 200, "v", message.MessageTypeInsert, false, true))
	cm.PublishReadProof([]message.ImmutableMessage{msg(20, message.MessageTypeInsert, false)}, tick(100))
	frontier, proof, err := cm.WaitReadProof(context.Background(), "v", 100)
	require.NoError(t, err)
	require.EqualValues(t, 20, frontier.GrowingTimetick)
	require.EqualValues(t, 10, frontier.TransformingTimetick)
	require.EqualValues(t, 100, proof)
	require.EqualValues(t, 200, cm.GetQueryMVCCOfVChannel("v").GrowingTimetick)

	// Uncommitted transaction bodies cannot become visible in the read proof.
	cm.PublishReadProof([]message.ImmutableMessage{msg(110, message.MessageTypeDelete, true)}, tick(120))
	frontier, proof, err = cm.WaitReadProof(context.Background(), "v", 120)
	require.NoError(t, err)
	require.EqualValues(t, 20, frontier.GrowingTimetick)
	require.EqualValues(t, 10, frontier.TransformingTimetick)
	require.EqualValues(t, 120, proof)
	cm.PublishReadProof([]message.ImmutableMessage{msg(130, message.MessageTypeCommitTxn, true)}, tick(140))
	frontier, _, err = cm.WaitReadProof(context.Background(), "v", 140)
	require.NoError(t, err)
	require.EqualValues(t, 130, frontier.GrowingTimetick)
	require.EqualValues(t, 130, frontier.TransformingTimetick)

	// Idle time refreshes CAS proof age without moving either executor frontier.
	cm.PublishReadProof(nil, tick(1000))
	idle, proof, err := cm.WaitReadProof(context.Background(), "v", 1000)
	require.NoError(t, err)
	require.Equal(t, frontier, idle)
	require.EqualValues(t, 1000, proof)
}

func TestReadProofWaitsForPublicationAndCancels(t *testing.T) {
	cm := NewMVCCManager(10)
	cm.ApplyRecoveryBarrier("v", 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := cm.WaitReadProof(ctx, "v", 100)
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := cm.WaitReadProof(ctx, "v", 100); done <- err }()
	confirmation := createQueryTestMessage(t, 100, "", message.MessageTypeTimeTick, false, false).IntoImmutableMessage(nil)
	cm.PublishReadProof(nil, confirmation)
	require.NoError(t, <-done)

	// A new WAL owner cannot reuse the old owner's volatile certificate.
	replacement := NewMVCCManager(100)
	replacement.ApplyRecoveryBarrier("v", 100)
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, _, err = replacement.WaitReadProof(canceled, "v", 100)
	require.ErrorIs(t, err, context.Canceled)
}
