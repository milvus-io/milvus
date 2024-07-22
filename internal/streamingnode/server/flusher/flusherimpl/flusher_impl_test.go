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

package flusherimpl

import (
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type FlusherSuite struct {
	integration.MiniClusterSuite
	flusher flusher.Flusher
}

func (s *FlusherSuite) SetupSuite() {
	paramtable.Init()
}

func (s *FlusherSuite) TearDownSuite() {}

func (s *FlusherSuite) SetupTest() {
	pp := &util.PipelineParams{
		Broker:             nil,
		SyncMgr:            nil,
		TimeTickSender:     nil,
		CompactionExecutor: nil,
		MsgStreamFactory:   nil,
		DispClient:         nil,
		ChunkManager:       nil,
		Session:            nil,
		WriteBufferManager: nil,
		CheckpointUpdater:  nil,
		Allocator:          nil,
	}
	s.flusher = NewFlusher(pp)
}

func (s *FlusherSuite) TestFlusher() {
	s.flusher.Start()
	defer s.flusher.Stop()

	vchannels := []string{
		"ch-0", "ch-1", "ch-2",
	}
	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().DropVirtualChannel()

	scanner := mock_wal.NewMockScanner(s.T())
	scanner.EXPECT().Chan().Return(make(chan message.ImmutableMessage, 1024))
	wal := mock_wal.NewMockWAL(s.T())
	wal.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner, nil)

	err := s.flusher.RegisterPChannel(wal)
	s.NoError(err)

	s.Eventuallyf(func() bool {
		return
	}, 10*time.Second, 100*time.Millisecond)
}

func TestFlusherSuite(t *testing.T) {
	suite.Run(t, new(FlusherSuite))
}
