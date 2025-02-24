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

package msgstream

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
)

func TestPulsarMsgUtil(t *testing.T) {
	pmsFactory := NewPmsFactory(&Params.ServiceParam)

	ctx := context.Background()
	msgStream, err := pmsFactory.NewMsgStream(ctx)
	assert.NoError(t, err)
	defer msgStream.Close()

	// create a topic
	msgStream.AsProducer(ctx, []string{"test"})

	UnsubscribeChannels(ctx, pmsFactory, "sub", []string{"test"})
}

func TestGetLatestMsgID(t *testing.T) {
	factory := NewMockMqFactory()
	ctx := context.Background()
	{
		factory.NewMsgStreamFunc = func(ctx context.Context) (MsgStream, error) {
			return nil, errors.New("mock")
		}
		_, err := GetChannelLatestMsgID(ctx, factory, "test")
		assert.Error(t, err)
	}
	stream := NewMockMsgStream(t)
	factory.NewMsgStreamFunc = func(ctx context.Context) (MsgStream, error) {
		return stream, nil
	}
	stream.EXPECT().Close().Return()

	{
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
		_, err := GetChannelLatestMsgID(ctx, factory, "test")
		assert.Error(t, err)
	}

	{
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().GetLatestMsgID(mock.Anything).Return(nil, errors.New("mock")).Once()
		_, err := GetChannelLatestMsgID(ctx, factory, "test")
		assert.Error(t, err)
	}

	{
		mockMsgID := common.NewMockMessageID(t)
		mockMsgID.EXPECT().Serialize().Return([]byte("mock")).Once()
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().GetLatestMsgID(mock.Anything).Return(mockMsgID, nil).Once()
		id, err := GetChannelLatestMsgID(ctx, factory, "test")
		assert.NoError(t, err)
		assert.Equal(t, []byte("mock"), id)
	}
}

func TestReplicateConfig(t *testing.T) {
	t.Run("get replicate id", func(t *testing.T) {
		{
			msg := &InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						ReplicateInfo: &commonpb.ReplicateInfo{
							ReplicateID: "local",
						},
					},
				},
			}
			assert.Equal(t, "local", GetReplicateID(msg))
			assert.True(t, MatchReplicateID(msg, "local"))
		}
		{
			msg := &InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{},
				},
			}
			assert.Equal(t, "", GetReplicateID(msg))
			assert.False(t, MatchReplicateID(msg, "local"))
		}
		{
			msg := &MarshalFailTsMsg{}
			assert.Equal(t, "", GetReplicateID(msg))
		}
	})

	t.Run("get replicate config", func(t *testing.T) {
		{
			assert.Nil(t, GetReplicateConfig("", "", ""))
		}
		{
			rc := GetReplicateConfig("local", "db", "col")
			assert.Equal(t, "local", rc.ReplicateID)
			checkFunc := rc.CheckFunc
			assert.False(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{},
			}))
			assert.True(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{
					IsEnd:     true,
					IsCluster: true,
				},
			}))
			assert.False(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{
					IsEnd:    true,
					Database: "db1",
				},
			}))
			assert.True(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{
					IsEnd:    true,
					Database: "db",
				},
			}))
			assert.False(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{
					IsEnd:      true,
					Database:   "db",
					Collection: "col1",
				},
			}))
		}
		{
			rc := GetReplicateConfig("local", "db", "col")
			checkFunc := rc.CheckFunc
			assert.True(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{
					IsEnd:    true,
					Database: "db",
				},
			}))
			assert.False(t, checkFunc(&ReplicateMsg{
				ReplicateMsg: &msgpb.ReplicateMsg{
					IsEnd:      true,
					Database:   "db1",
					Collection: "col1",
				},
			}))
		}
	})
}
