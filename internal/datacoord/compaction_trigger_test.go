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

package datacoord

import (
	"context"
	satomic "sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
)

func Test_TirggerCompaction_WaitResult(t *testing.T) {
	originValue := Params.DataCoordCfg.EnableAutoCompaction.GetValue()
	Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer func() {
		Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, originValue)
	}()
	triggerStore := metacache.NewMetaStore(nil)
	m := &meta.Meta{
		Segments:  meta.NewSegmentsInfo(triggerStore),
		MetaStore: triggerStore,
	}
	got := compaction.NewCompactionTrigger(m, nil, &compaction.Inspector{}, meta.NewMockAllocator(t),
		&ServerHandler{
			&Server{
				meta: m,
			},
		}, index.NewTestVersionManager())
	got.Signals = make(chan *compaction.CompactionSignal, 1)
	{
		_, err := got.TriggerCompaction(context.TODO(), compaction.NewCompactionSignal().
			WithCollectionID(1).
			WithPartitionID(1).
			WithSegmentIDs(1).
			WithChannel("a").
			WithWaitResult(false))
		assert.NoError(t, err)
	}
	{
		_, err := got.TriggerCompaction(context.TODO(), compaction.NewCompactionSignal().
			WithCollectionID(2).
			WithPartitionID(2).
			WithSegmentIDs(2).
			WithChannel("b").
			WithWaitResult(false))
		assert.Error(t, err)
	}
	var i satomic.Value
	i.Store(0)
	check := func() {
		for {
			select {
			case signal := <-got.Signals:
				x := i.Load().(int)
				i.Store(x + 1)
				assert.EqualValues(t, 1, signal.CollectionID)
			default:
				return
			}
		}
	}
	check()
	assert.Equal(t, 1, i.Load().(int))

	var j satomic.Value
	j.Store(0)
	go func() {
		timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		defer cancelFunc()
		for {
			select {
			case signal := <-got.Signals:
				x := j.Load().(int)
				j.Store(x + 1)
				switch x {
				case 0:
					assert.EqualValues(t, 3, signal.CollectionID)
				case 1:
					assert.EqualValues(t, 4, signal.CollectionID)
				}
				signal.Notify(nil)
			case <-timeoutCtx.Done():
				return
			}
		}
	}()

	{
		_, err := got.TriggerCompaction(context.TODO(), compaction.NewCompactionSignal().
			WithCollectionID(3).
			WithPartitionID(3).
			WithSegmentIDs(3).
			WithChannel("c").
			WithWaitResult(false))
		assert.NoError(t, err)
	}

	{
		_, err := got.TriggerCompaction(context.TODO(), compaction.NewCompactionSignal().
			WithCollectionID(4).
			WithPartitionID(4).
			WithSegmentIDs(4).
			WithChannel("d").
			WithWaitResult(true))
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		return j.Load().(int) == 2
	}, 2*time.Second, 500*time.Millisecond)
}
