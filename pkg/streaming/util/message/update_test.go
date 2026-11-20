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

package message_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestPKUpdateMessage(t *testing.T) {
	body := &messagespb.UpdateMessageBody{PartitionId: -1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}}, Assignments: []*messagespb.UpdateAssignment{
		{FieldId: 101, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}},
		{FieldId: 102, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: ""}}}}},
	}}
	m := message.NewUpdateMessageBuilderV2().WithVChannel("p_10v0").WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 10, SchemaVersion: proto.Int32(7)}).WithBody(body).MustBuildMutable()
	u, err := message.AsMutableUpdateMessageV2(m)
	require.NoError(t, err)
	require.True(t, proto.Equal(body, u.MustBody()))
	require.True(t, m.MessageType().Valid())
	require.True(t, m.MessageType().IsDMLMessageType())
	require.True(t, m.MessageType().CanEnableCipher())
	require.False(t, m.MessageType().IsSystem())
	require.False(t, m.MessageType().IsSelfControlled())
	require.False(t, m.MessageType().IsExclusiveRequired())
	require.False(t, message.PKUpdateEnabled, "consumer integration is a separate activation gate")
}
