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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type noopStatsUpdater struct{}

func (noopStatsUpdater) Update(string, typeutil.Timestamp, []*commonpb.SegmentStats) {}

func (noopStatsUpdater) GetLatestTimestamp(string) typeutil.Timestamp { return 0 }

func TestWriteNodeSkipsSchemaLookupForDeleteOnlyBatch(t *testing.T) {
	const vchannel = "test-vchannel"

	metaCache := metacache.NewMockMetaCache(t)
	metaCache.EXPECT().
		GetSchema(mock.Anything).
		Return(&schemapb.CollectionSchema{Version: 42}).
		Maybe()

	bufferManager := writebuffer.NewMockBufferManager(t)
	deleteMessages := []*msgstream.DeleteMsg{{}}
	bufferManager.EXPECT().
		BufferData(
			vchannel,
			mock.MatchedBy(func(data []*writebuffer.InsertData) bool { return len(data) == 0 }),
			deleteMessages,
			mock.Anything,
			mock.Anything,
			int32(0),
		).
		Return(nil).
		Once()

	functionStore := function.NewFunctionRunnerLocalStore()
	defer functionStore.Close()

	node := &writeNode{
		channelName:   vchannel,
		wbManager:     bufferManager,
		updater:       noopStatsUpdater{},
		metacache:     metaCache,
		functionStore: functionStore,
	}
	msg := &FlowGraphMsg{
		DeleteMessages: deleteMessages,
		StartPositions: []*msgpb.MsgPosition{{ChannelName: "test-pchannel", Timestamp: 100}},
		EndPositions:   []*msgpb.MsgPosition{{ChannelName: "test-pchannel", Timestamp: 200}},
	}

	output := node.Operate([]Msg{msg})

	require.Len(t, output, 1)
	metaCache.AssertNotCalled(t, "GetSchema", mock.Anything)
}
