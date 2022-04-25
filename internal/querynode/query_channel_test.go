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

package querynode

import (
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/mock"
)

type mockQueryMsgStream struct {
	mock.Mock
}

func (m *mockQueryMsgStream) Start() {
	m.Called()
}

func (m *mockQueryMsgStream) Close() {
	m.Called()
}

func (m *mockQueryMsgStream) AsProducer(channels []string) {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) Produce(_ *msgstream.MsgPack) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc) {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) ComputeProduceChannelIndexes(tsMsgs []msgstream.TsMsg) [][]int32 {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) GetProduceChannels() []string {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) ProduceMark(_ *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) Broadcast(_ *msgstream.MsgPack) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) BroadcastMark(_ *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) AsConsumer(channels []string, subName string) {
	m.Called(channels, subName)
}

func (m *mockQueryMsgStream) AsConsumerWithPosition(channels []string, subName string, position mqwrapper.SubscriptionInitialPosition) {
	panic("not implemented") // TODO: Implement
}

func (m *mockQueryMsgStream) Chan() <-chan *msgstream.MsgPack {
	args := m.Called()
	return args.Get(0).(<-chan *msgstream.MsgPack)
}

func (m *mockQueryMsgStream) Seek(offset []*msgstream.MsgPosition) error {
	args := m.Called(offset)
	return args.Error(0)
}

func (m *mockQueryMsgStream) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	panic("not implemented") // TODO: Implement
}

func TestQueryChannel_AsConsumer(t *testing.T) {
	t.Run("AsConsumer with no seek", func(t *testing.T) {
		mqs := &mockQueryMsgStream{}
		mqs.On("Close").Return()

		qc := NewQueryChannel(defaultCollectionID, nil, mqs, nil)

		mqs.On("AsConsumer", []string{defaultDMLChannel}, defaultSubName).Return()

		qc.AsConsumer(defaultDMLChannel, defaultSubName, nil)
		qc.Stop()

		mqs.AssertCalled(t, "AsConsumer", []string{defaultDMLChannel}, defaultSubName)
		mqs.AssertNotCalled(t, "Seek")

		mqs.AssertExpectations(t)

		qc.Stop()
	})

	t.Run("AsConsumer with bad position", func(t *testing.T) {
		mqs := &mockQueryMsgStream{}
		mqs.On("Close").Return()

		qc := NewQueryChannel(defaultCollectionID, nil, mqs, nil)

		mqs.On("AsConsumer", []string{defaultDMLChannel}, defaultSubName).Return()

		qc.AsConsumer(defaultDMLChannel, defaultSubName, &internalpb.MsgPosition{})
		qc.Stop()

		mqs.AssertCalled(t, "AsConsumer", []string{defaultDMLChannel}, defaultSubName)
		mqs.AssertNotCalled(t, "Seek")

		mqs.AssertExpectations(t)

	})

	t.Run("AsConsumer with position", func(t *testing.T) {
		mqs := &mockQueryMsgStream{}
		mqs.On("Close").Return()

		qc := NewQueryChannel(defaultCollectionID, nil, mqs, nil)

		msgID := make([]byte, 8)
		rand.Read(msgID)
		pos := &internalpb.MsgPosition{MsgID: msgID}

		mqs.On("AsConsumer", []string{defaultDMLChannel}, defaultSubName).Return()
		mqs.On("Seek", []*internalpb.MsgPosition{pos}).Return(nil)

		qc.AsConsumer(defaultDMLChannel, defaultSubName, pos)

		qc.Stop()

		mqs.AssertCalled(t, "AsConsumer", []string{defaultDMLChannel}, defaultSubName)
		mqs.AssertCalled(t, "Seek", []*internalpb.MsgPosition{pos})

		mqs.AssertExpectations(t)

	})

}
