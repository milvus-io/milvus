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

package pipeline

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type mockMsgStreamFactory struct {
	InitReturnNil       bool
	NewMsgStreamNoError bool
}

var (
	_ msgstream.Factory  = &mockMsgStreamFactory{}
	_ dependency.Factory = (*mockMsgStreamFactory)(nil)
)

func (mm *mockMsgStreamFactory) Init(params *paramtable.ComponentParam) {}
func (mm *mockMsgStreamFactory) NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return nil, nil
}

func (mm *mockMsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if !mm.NewMsgStreamNoError {
		return nil, errors.New("New MsgStream error")
	}
	return &mockTtMsgStream{}, nil
}

func (mm *mockMsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return &mockTtMsgStream{}, nil
}

func (mm *mockMsgStreamFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return nil
}

type mockTtMsgStream struct{}

func (mtm *mockTtMsgStream) SetReplicate(config *msgstream.ReplicateConfig) {
}

func (mtm *mockTtMsgStream) Close() {}

func (mtm *mockTtMsgStream) Chan() <-chan *msgstream.ConsumeMsgPack {
	return make(chan *msgstream.ConsumeMsgPack, 100)
}

func (mtm *mockTtMsgStream) AsProducer(ctx context.Context, channels []string) {}

func (mtm *mockTtMsgStream) AsConsumer(ctx context.Context, channels []string, subName string, position common.SubscriptionInitialPosition) error {
	return nil
}

func (mtm *mockTtMsgStream) GetUnmarshalDispatcher() msgstream.UnmarshalDispatcher {
	return nil
}

func (mtm *mockTtMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc) {}

func (mtm *mockTtMsgStream) GetProduceChannels() []string {
	return make([]string, 0)
}

func (mtm *mockTtMsgStream) Produce(context.Context, *msgstream.MsgPack) error {
	return nil
}

func (mtm *mockTtMsgStream) Broadcast(context.Context, *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	return nil, nil
}

func (mtm *mockTtMsgStream) Seek(ctx context.Context, msgPositions []*msgstream.MsgPosition, includeCurrentMsg bool) error {
	return nil
}

func (mtm *mockTtMsgStream) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	return nil, nil
}

func (mtm *mockTtMsgStream) CheckTopicValid(channel string) error {
	return nil
}

func (mtm *mockTtMsgStream) ForceEnableProduce(can bool) {
}

func TestNewDmInputNode(t *testing.T) {
	assert.Panics(t, func() {
		newDmInputNode(&nodeConfig{
			msFactory:    &mockMsgStreamFactory{},
			vChannelName: "mock_vchannel_0",
		}, nil)
	})

	node := newDmInputNode(&nodeConfig{
		msFactory:    &mockMsgStreamFactory{},
		vChannelName: "mock_vchannel_0",
	}, make(<-chan *msgstream.MsgPack))
	assert.NotNil(t, node)
}
