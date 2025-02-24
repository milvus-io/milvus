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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type positionGenerator func(channelName string, timestamp uint64, msgGroup string, targetMsgIDs []uint64) []*msgpb.MsgPosition

func TestNmq(t *testing.T) {
	storeDir, err := os.MkdirTemp("", "milvus_mq_nmq")
	assert.NoError(t, err)
	defer os.RemoveAll(storeDir)

	paramtable.Init()
	cfg := nmq.ParseServerOption(paramtable.Get())
	cfg.Opts.StoreDir = storeDir
	nmq.MustInitNatsMQ(cfg)
	defer nmq.CloseNatsMQ()

	f1 := NewNatsmqFactory()
	f2 := NewNatsmqFactory()

	client, err := nmq.NewClientWithDefaultOptions(context.Background())
	if err != nil {
		panic(err)
	}

	testMQ(t, client, []Factory{f1, f2}, func(channelName string, timestamp uint64, msgGroup string, targetMsgIDs []uint64) []*msgpb.MsgPosition {
		result := make([]*msgpb.MsgPosition, 0, len(targetMsgIDs))

		for _, targetMsgID := range targetMsgIDs {
			msgID := nmq.NewNmqID(targetMsgID).Serialize()
			result = append(result, &msgpb.MsgPosition{
				ChannelName: channelName,
				Timestamp:   timestamp,
				MsgGroup:    msgGroup,
				MsgID:       msgID,
			})
		}
		return result
	})
}

func testMQ(t *testing.T, client mqwrapper.Client, factories []Factory, pg positionGenerator) {
	testStreamOperation(t, client)
	testFactoryCommonOperation(t, factories[0])
	testMsgStreamOperation(t, factories, pg)
}

// testFactoryOperation test common factory operation.
func testFactoryCommonOperation(t *testing.T, f Factory) {
	var err error
	ctx := context.Background()
	_, err = f.NewMsgStream(ctx)
	assert.NoError(t, err)

	_, err = f.NewTtMsgStream(ctx)
	assert.NoError(t, err)

	err = f.NewMsgStreamDisposer(ctx)([]string{"hello"}, "xx")
	assert.NoError(t, err)
}
