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

package rootcoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
)

func Test_waitForTsSyncedStep_Execute(t *testing.T) {
	// Params.InitOnce()
	// Params.ProxyCfg.TimeTickInterval = time.Millisecond

	ticker := newRocksMqTtSynchronizer()
	core := newTestCore(withTtSynchronizer(ticker))
	core.chanTimeTick.syncedTtHistogram.update("ch1", 100)
	s := &waitForTsSyncedStep{
		baseStep: baseStep{core: core},
		ts:       101,
		channel:  "ch1",
	}
	children, err := s.Execute(context.Background())
	assert.Equal(t, 0, len(children))
	assert.Error(t, err)
	core.chanTimeTick.syncedTtHistogram.update("ch1", 102)
	children, err = s.Execute(context.Background())
	assert.Equal(t, 0, len(children))
	assert.NoError(t, err)
}

func restoreConfirmGCInterval() {
	confirmGCInterval = time.Minute * 20
}

func Test_confirmGCStep_Execute(t *testing.T) {
	t.Run("wait for reschedule", func(t *testing.T) {
		confirmGCInterval = time.Minute * 1000
		defer restoreConfirmGCInterval()

		s := &confirmGCStep{lastScheduledTime: time.Now()}
		_, err := s.Execute(context.TODO())
		assert.Error(t, err)
	})

	t.Run("GC not finished", func(t *testing.T) {
		broker := newMockBroker()
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool {
			return false
		}

		core := newTestCore(withBroker(broker))

		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()

		s := newConfirmGCStep(core, 100, 1000)
		time.Sleep(confirmGCInterval)

		_, err := s.Execute(context.TODO())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		broker := newMockBroker()
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool {
			return true
		}

		core := newTestCore(withBroker(broker))

		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()

		s := newConfirmGCStep(core, 100, 1000)
		time.Sleep(confirmGCInterval)

		_, err := s.Execute(context.TODO())
		assert.NoError(t, err)
	})
}

func TestSkip(t *testing.T) {
	{
		s := &unwatchChannelsStep{isSkip: true}
		_, err := s.Execute(context.Background())
		assert.NoError(t, err)
	}

	{
		s := &deleteCollectionDataStep{isSkip: true}
		_, err := s.Execute(context.Background())
		assert.NoError(t, err)
	}

	{
		s := &deletePartitionDataStep{isSkip: true}
		_, err := s.Execute(context.Background())
		assert.NoError(t, err)
	}
}

func TestBroadcastCreatePartitionMsgStep(t *testing.T) {
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().AppendMessagesWithOption(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(streaming.AppendResponses{})
	streaming.SetWALForTest(wal)

	step := &broadcastCreatePartitionMsgStep{
		baseStep:  baseStep{core: nil},
		vchannels: []string{"ch-0", "ch-1"},
		partition: &model.Partition{
			CollectionID: 1,
			PartitionID:  2,
		},
	}
	t.Logf("%v\n", step.Desc())
	_, err := step.Execute(context.Background())
	assert.NoError(t, err)
}
