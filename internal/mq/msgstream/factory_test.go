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

package msgstream_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	nmqserver "github.com/milvus-io/milvus/internal/mq/mqimpl/natsmq/server"
	rmqserver "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// TODO: Should be refactored, MQ package use many global singleton variable.
// It's hard to maintain a clean unit test
func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

func TestNmq(t *testing.T) {
	storeDir, err := os.MkdirTemp("", fmt.Sprintf("milvus_mq_%s_123", msgstream.MsgStreamTypeNmq))
	assert.Nil(t, err)
	defer os.RemoveAll(storeDir)
	testMQ(t, msgstream.MsgStreamTypeNmq, storeDir)
}

func TestRmq(t *testing.T) {
	storeDir, err := os.MkdirTemp("", fmt.Sprintf("milvus_mq_%s_123", msgstream.MsgStreamTypeRmq))
	assert.Nil(t, err)
	defer os.RemoveAll(storeDir)
	testMQ(t, msgstream.MsgStreamTypeRmq, storeDir)
}

func testMQ(t *testing.T, st msgstream.StreamType, storeDir string) {
	var client mqwrapper.Client

	switch st {
	case msgstream.MsgStreamTypeRmq:
		err := rmqserver.InitRocksMQ(storeDir)
		assert.Nil(t, err)
		client, err = rmq.NewClientWithDefaultOptions()
		assert.Nil(t, err)
		defer rmqserver.CloseRocksMQ()
	case msgstream.MsgStreamTypeNmq:
		err := nmqserver.InitNatsMQ(storeDir)
		// assert.Nil(t, err)
		client, err = nmq.NewClientWithDefaultOptions()
		assert.Nil(t, err)
		defer nmqserver.CloseNatsMQ()
	default:
		panic("unreachable")
	}

	testStreamOperation(t, client)
	testFactoryCommonOperation(t, st, storeDir)
	testMsgStreamOperation(t, st, storeDir)
}

// testFactoryOperation test common factory operation.
func testFactoryCommonOperation(t *testing.T, st msgstream.StreamType, storeDir string) {
	var err error
	f := msgstream.NewFactory(st, storeDir)
	ctx := context.Background()
	_, err = f.NewMsgStream(ctx)
	assert.Nil(t, err)

	_, err = f.NewTtMsgStream(ctx)
	assert.Nil(t, err)

	_, err = f.NewQueryMsgStream(ctx)
	assert.Nil(t, err)

	err = f.NewMsgStreamDisposer(ctx)([]string{"hello"}, "xx")
	assert.Nil(t, err)
}
