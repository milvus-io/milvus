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
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_waitForTsSyncedStep_Execute(t *testing.T) {
	//Params.InitOnce()
	//Params.ProxyCfg.TimeTickInterval = time.Millisecond

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

	t.Run("test release collection step", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		broker := NewMockBroker(t)
		core := &Core{
			queryCoord: qc,
			broker:     broker,
		}

		releaseCollectionStep := &releaseCollectionStep{
			baseStep:     baseStep{core: core},
			collectionID: 1,
		}

		// test release failed, but get components success, expected error
		broker.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(errors.New("fake error"))
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{}, nil)
		_, err := releaseCollectionStep.Execute(context.TODO())
		assert.Error(t, err)

		// test release failed, and get components failed, expected nil
		qc.ExpectedCalls = nil
		broker.ExpectedCalls = nil
		broker.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(errors.New("fake error"))
		qc.EXPECT().GetComponentStates(mock.Anything).Return(nil, errors.New("fake error"))
		_, err = releaseCollectionStep.Execute(context.TODO())
		assert.NoError(t, err)

		// test release failed, but retry success, expected nil
		qc.ExpectedCalls = nil
		broker.ExpectedCalls = nil
		broker.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(errors.New("fake error")).Times(3)
		broker.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(nil)
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{}, nil)
		_, err = releaseCollectionStep.Execute(context.TODO())
		assert.NoError(t, err)

		// test release succeed expected nil
		qc.ExpectedCalls = nil
		broker.ExpectedCalls = nil
		broker.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(nil)
		_, err = releaseCollectionStep.Execute(context.TODO())
		assert.NoError(t, err)
	})
}
