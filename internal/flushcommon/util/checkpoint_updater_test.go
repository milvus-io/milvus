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

package util

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ChannelCPUpdaterSuite struct {
	suite.Suite

	broker  *broker.MockBroker
	updater *ChannelCheckpointUpdater
}

func (s *ChannelCPUpdaterSuite) SetupTest() {
	paramtable.Init()
	s.broker = broker.NewMockBroker(s.T())
	s.updater = NewChannelCheckpointUpdater(s.broker)
}

func (s *ChannelCPUpdaterSuite) TestUpdate() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.ChannelCheckpointUpdateTickInSeconds.Key, "0.01")
	defer paramtable.Get().Save(paramtable.Get().DataNodeCfg.ChannelCheckpointUpdateTickInSeconds.Key, "10")

	s.broker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, positions []*msgpb.MsgPosition) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	go s.updater.Start()
	defer s.updater.Close()

	tasksNum := 100000
	counter := atomic.NewInt64(0)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < tasksNum; i++ {
			// add duplicated task with same timestamp
			for j := 0; j < 10; j++ {
				s.updater.AddTask(&msgpb.MsgPosition{
					ChannelName: fmt.Sprintf("ch-%d", i),
					MsgID:       []byte{0},
					Timestamp:   100,
				}, false, func() {
					counter.Add(1)
				})
			}
		}
	}()
	wg.Wait()
	s.Eventually(func() bool {
		return counter.Load() == int64(tasksNum)
	}, time.Second*10, time.Millisecond*100)
}

func TestChannelCPUpdater(t *testing.T) {
	suite.Run(t, new(ChannelCPUpdaterSuite))
}
