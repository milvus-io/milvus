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

package streaming

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestPKUpdateSplitTransaction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "commit"
		if fail {
			name = "body_failure"
		}
		t.Run(name, func(t *testing.T) {
			w, _, _, handler := createMockWAL(t)
			defer w.Close()
			p := mock_producer.NewMockProducer(t)
			p.EXPECT().IsAvailable().Return(true).Maybe()
			p.EXPECT().Available().Return(make(chan struct{})).Maybe()
			p.EXPECT().Close().Return().Maybe()
			var bodies, commits atomic.Int32
			p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, m message.MutableMessage) (*types.AppendResult, error) {
				switch m.MessageType() {
				case message.MessageTypeBeginTxn:
					return &types.AppendResult{TxnCtx: &message.TxnContext{TxnID: 100, Keepalive: time.Second}}, nil
				case message.MessageTypeUpdate:
					bodies.Add(1)
					require.Equal(t, message.TxnID(100), m.TxnContext().TxnID)
					if fail {
						return nil, status.NewSchemaVersionMismatch("schema advanced")
					}
					return &types.AppendResult{TimeTick: 101}, nil
				case message.MessageTypeCommitTxn:
					commits.Add(1)
					require.EqualValues(t, 2, message.MustAsMutableCommitTxnMessageV2(m).Header().GetBodyCnt())
					return &types.AppendResult{MessageID: walimplstest.NewTestMessageID(200), TimeTick: 200}, nil
				default:
					t.Errorf("unexpected message %s", m.MessageType())
					return nil, status.NewInner("unexpected type")
				}
			}).Maybe()
			handler.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil)
			var msgs []message.MutableMessage
			for _, pk := range []int64{1, 2} {
				msgs = append(msgs, message.NewUpdateMessageBuilderV2().WithVChannel(vChannel1).
					WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 10, SchemaVersion: proto.Int32(7)}).
					WithBody(&messagespb.UpdateMessageBody{PartitionId: -1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}}}, Assignments: []*messagespb.UpdateAssignment{{FieldId: 101, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}}}}).MustBuildMutable())
			}
			resp := w.AppendMessages(t.Context(), msgs...)
			if fail {
				require.Error(t, resp.UnwrapFirstError())
				require.True(t, status.AsStreamingError(resp.UnwrapFirstError()).IsSchemaVersionMismatch())
				require.Zero(t, commits.Load(), "failed transaction bodies must never be committed")
			} else {
				require.NoError(t, resp.UnwrapFirstError())
				require.EqualValues(t, 2, bodies.Load())
				require.EqualValues(t, 1, commits.Load())
				for _, r := range resp.Responses {
					require.EqualValues(t, 200, r.AppendResult.TimeTick)
				}
			}
		})
	}
}
