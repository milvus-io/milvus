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

package metacache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
)

// TestChannelCheckpointAcceptance pins the two conditions the batch update
// shares with the DataCoord code it replaced: a WoodPecker position carries no
// msgID and must still be persisted, and a position that repeats the previous
// timestamp with a different msgID still advances the checkpoint.
func TestChannelCheckpointAcceptance(t *testing.T) {
	const channel = "ch-0"

	tests := []struct {
		name string
		old  *msgpb.MsgPosition
		pos  *msgpb.MsgPosition
		want bool
	}{
		{
			name: "no previous checkpoint",
			pos:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{1}, Timestamp: 10},
			want: true,
		},
		{
			name: "newer timestamp",
			old:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{1}, Timestamp: 10},
			pos:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{2}, Timestamp: 11},
			want: true,
		},
		{
			name: "older timestamp",
			old:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{2}, Timestamp: 11},
			pos:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{1}, Timestamp: 10},
			want: false,
		},
		{
			name: "same timestamp, same msgID",
			old:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{1}, Timestamp: 10},
			pos:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{1}, Timestamp: 10},
			want: false,
		},
		{
			name: "same timestamp, different msgID",
			old:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{1}, Timestamp: 10},
			pos:  &msgpb.MsgPosition{ChannelName: channel, MsgID: []byte{2}, Timestamp: 10},
			want: true,
		},
		{
			name: "woodpecker position without msgID",
			pos:  &msgpb.MsgPosition{ChannelName: channel, WALName: commonpb.WALName_WoodPecker, Timestamp: 10},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := mocks.NewDataCoordCatalog(t)
			var saved []*msgpb.MsgPosition
			catalog.EXPECT().SaveChannelCheckpoints(mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, positions []*msgpb.MsgPosition) error {
					saved = append(saved, positions...)
					return nil
				}).Maybe()

			s := NewMetaStore(catalog)
			if test.old != nil {
				s.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{channel: test.old})
			}

			require.NoError(t, s.UpdateChannelCheckpoints(context.Background(), []*msgpb.MsgPosition{test.pos}))
			assert.Equal(t, test.want, len(saved) == 1, "persisted=%v", saved)
			if test.want {
				assert.Equal(t, test.pos.GetTimestamp(), s.GetChannelCheckpoint(channel).GetTimestamp())
			}
		})
	}
}

// TestChannelCheckpointRejectsIllegal asserts the positions that must never be
// persisted: nil, no channel name, and a non-WoodPecker position with no msgID.
func TestChannelCheckpointRejectsIllegal(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	s := NewMetaStore(catalog)

	require.NoError(t, s.UpdateChannelCheckpoints(context.Background(), []*msgpb.MsgPosition{
		nil,
		{MsgID: []byte{1}, Timestamp: 10},
		{ChannelName: "ch-0", Timestamp: 10},
	}))
	assert.Empty(t, s.GetChannelCheckpoints())
}
