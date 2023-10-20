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

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

func TestPulsarMsgUtil(t *testing.T) {
	pmsFactory := NewPmsFactory(&Params.ServiceParam)

	ctx := context.Background()
	msgStream, err := pmsFactory.NewMsgStream(ctx)
	assert.NoError(t, err)
	defer msgStream.Close()

	// create a topic
	msgStream.AsProducer([]string{"test"})

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
		mockMsgID := mqwrapper.NewMockMessageID(t)
		mockMsgID.EXPECT().Serialize().Return([]byte("mock")).Once()
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().GetLatestMsgID(mock.Anything).Return(mockMsgID, nil).Once()
		id, err := GetChannelLatestMsgID(ctx, factory, "test")
		assert.NoError(t, err)
		assert.Equal(t, []byte("mock"), id)
	}
}
