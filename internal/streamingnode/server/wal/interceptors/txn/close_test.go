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

package txn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestCloseReleasesReferencesWithoutResolvingUncertainCommit(t *testing.T) {
	metrics := metricsutil.NewTxnMetrics("upsert-close")
	defer metrics.Close()
	s := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: time.Second}, 0, metrics.BeginTxn())
	body := message.NewUpsertMessageBuilderV2().WithVChannel(s.VChannel()).
		WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().IntoImmutableMessage(nil)
	s.RecordSuccessfulBody(body)
	require.NoError(t, s.RequestCommitAndWait(context.Background(), 0, 0))
	cleaned := false
	s.RegisterCleanup(func() { cleaned = true }, 0)
	m := &TxnManager{sessions: map[message.TxnID]*TxnSession{1: s}, recoveredSessions: map[message.TxnID]struct{}{1: {}}, recoveredSessionsDoneChan: make(chan struct{})}
	m.Close()
	require.Equal(t, message.TxnStateOnCommit, s.State(), "WAL recovery owns the ambiguous outcome")
	require.True(t, cleaned)
	require.Empty(t, s.SuccessfulBodies())
	require.Empty(t, m.sessions)
	require.Empty(t, m.recoveredSessions)
	select {
	case <-m.RecoverDone():
	default:
		t.Fatal("close must release recovery waiters")
	}
	m.Close()
}
