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

package datanode

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

var flushTestDir = "/tmp/milvus_test/flush"

type emptyFlushTask struct{}

func (t *emptyFlushTask) flushInsertData() error {
	return nil
}

func (t *emptyFlushTask) flushDeleteData() error {
	return nil
}

type errFlushTask struct{}

func (t *errFlushTask) flushInsertData() error {
	return errors.New("mocked error")
}

func (t *errFlushTask) flushDeleteData() error {
	return errors.New("mocked error")
}

func TestOrderFlushQueue_Execute(t *testing.T) {
	counter := atomic.Int64{}
	finish := sync.WaitGroup{}

	size := 1000
	finish.Add(size)
	q := newOrderFlushQueue(1, func(*segmentFlushPack) {
		counter.Inc()
		finish.Done()
	})

	q.init()
	ids := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		id := make([]byte, 10)
		rand.Read(id)
		ids = append(ids, id)
	}

	wg := sync.WaitGroup{}
	wg.Add(2 * size)
	for i := 0; i < size; i++ {
		go func(id []byte) {
			q.enqueueDelFlush(&emptyFlushTask{}, &DelDataBuf{}, &msgpb.MsgPosition{
				MsgID: id,
			})
			wg.Done()
		}(ids[i])
		go func(id []byte) {
			q.enqueueInsertFlush(&emptyFlushTask{}, map[UniqueID]*datapb.Binlog{}, map[UniqueID]*datapb.Binlog{}, false, false, &msgpb.MsgPosition{
				MsgID: id,
			})
			wg.Done()
		}(ids[i])
	}
	wg.Wait()
	finish.Wait()

	assert.EqualValues(t, size, counter.Load())
}

func TestOrderFlushQueue_Order(t *testing.T) {
	counter := atomic.Int64{}
	finish := sync.WaitGroup{}

	size := 1000
	finish.Add(size)
	resultList := make([][]byte, 0, size)
	q := newOrderFlushQueue(1, func(pack *segmentFlushPack) {
		counter.Inc()
		resultList = append(resultList, pack.pos.MsgID)
		finish.Done()
	})

	q.init()
	ids := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		id := make([]byte, 10)
		rand.Read(id)
		ids = append(ids, id)
	}

	wg := sync.WaitGroup{}
	wg.Add(size)
	for i := 0; i < size; i++ {
		q.enqueueDelFlush(&emptyFlushTask{}, &DelDataBuf{}, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		q.enqueueInsertFlush(&emptyFlushTask{}, map[UniqueID]*datapb.Binlog{}, map[UniqueID]*datapb.Binlog{}, false, false, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		wg.Done()
	}
	wg.Wait()
	finish.Wait()

	assert.EqualValues(t, size, counter.Load())

	require.Equal(t, size, len(resultList))
	for i := 0; i < size; i++ {
		assert.EqualValues(t, ids[i], resultList[i])
	}
}

func newTestChannel() *ChannelMeta {
	return &ChannelMeta{
		segments:     make(map[UniqueID]*Segment),
		collectionID: 1,
		collSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{
				FieldID:      100,
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			}},
		},
	}
}

func TestRendezvousFlushManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	size := 1000
	var counter atomic.Int64
	alloc := allocator.NewMockAllocator(t)
	channel := newTestChannel()
	hisotricalStats := storage.NewPrimaryKeyStats(100, 5, 10)
	testSeg := &Segment{
		collectionID: 1,
		segmentID:    3,
		// flush segment will merge all historial stats
		historyStats: []*storage.PkStatistics{
			{
				PkFilter: hisotricalStats.BF,
				MinPK:    hisotricalStats.MinPk,
				MaxPK:    hisotricalStats.MaxPk,
			},
		},
	}
	testSeg.setType(datapb.SegmentType_New)
	channel.segments[testSeg.segmentID] = testSeg

	m := NewRendezvousFlushManager(alloc, cm, channel, func(pack *segmentFlushPack) {
		counter.Inc()
	}, emptyFlushAndDropFunc)

	ids := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		id := make([]byte, 10)
		rand.Read(id)
		ids = append(ids, id)
	}

	for i := 0; i < size; i++ {
		err := m.flushDelData(nil, testSeg.segmentID, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)

		_, err = m.flushBufferData(nil, testSeg.segmentID, true, false, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool { return counter.Load() == int64(size) }, 3*time.Second, 100*time.Millisecond)

	_, _, err := m.serializePkStatsLog(0, false, nil, &storage.InsertCodec{Schema: &etcdpb.CollectionMeta{Schema: &schemapb.CollectionSchema{}, ID: 0}})
	assert.Error(t, err)
}

func TestRendezvousFlushManager_Inject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	size := 1000
	var counter atomic.Int64
	var packMut sync.Mutex
	packs := make([]*segmentFlushPack, 0, size+3)
	alloc := allocator.NewMockAllocator(t)

	channel := newTestChannel()
	segments := []*Segment{{
		collectionID: 1,
		segmentID:    1,
	}, {
		collectionID: 1,
		segmentID:    2,
	}, {
		collectionID: 1,
		segmentID:    3,
	}}

	for _, seg := range segments {
		seg.setType(datapb.SegmentType_New)
		channel.segments[seg.segmentID] = seg
	}

	m := NewRendezvousFlushManager(alloc, cm, channel, func(pack *segmentFlushPack) {
		packMut.Lock()
		packs = append(packs, pack)
		packMut.Unlock()
		counter.Inc()
	}, emptyFlushAndDropFunc)

	ti := newTaskInjection(1, func(*segmentFlushPack) {})
	m.injectFlush(ti, 1)
	<-ti.injected
	ti.injectDone(true)

	ids := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		id := make([]byte, 10)
		rand.Read(id)
		ids = append(ids, id)
	}

	for i := 0; i < size; i++ {
		err := m.flushDelData(nil, 1, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)

		_, err = m.flushBufferData(nil, 1, true, false, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool { return counter.Load() == int64(size) }, 3*time.Second, 100*time.Millisecond)

	id := make([]byte, 10)
	rand.Read(id)
	id2 := make([]byte, 10)
	rand.Read(id2)
	_, err := m.flushBufferData(nil, 2, true, false, &msgpb.MsgPosition{
		MsgID: id,
	})

	assert.NoError(t, err)
	_, err = m.flushBufferData(nil, 3, true, false, &msgpb.MsgPosition{
		MsgID: id2,
	})
	assert.NoError(t, err)

	ti = newTaskInjection(2, func(pack *segmentFlushPack) {
		pack.segmentID = 4
	})
	m.injectFlush(ti, 2, 3)

	err = m.flushDelData(nil, 2, &msgpb.MsgPosition{
		MsgID: id,
	})
	assert.NoError(t, err)

	err = m.flushDelData(nil, 3, &msgpb.MsgPosition{
		MsgID: id2,
	})
	assert.NoError(t, err)

	<-ti.Injected()
	ti.injectDone(true)

	assert.Eventually(t, func() bool { return counter.Load() == int64(size+2) }, 3*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 4, packs[size].segmentID)

	rand.Read(id)

	_, err = m.flushBufferData(nil, 2, false, false, &msgpb.MsgPosition{
		MsgID: id,
	})
	assert.NoError(t, err)

	ti = newTaskInjection(1, func(pack *segmentFlushPack) {
		pack.segmentID = 5
	})
	go func() {
		<-ti.injected
		ti.injectDone(false) // inject fail, segment id shall not be changed to 5
	}()
	m.injectFlush(ti, 2)

	m.flushDelData(nil, 2, &msgpb.MsgPosition{
		MsgID: id,
	})
	assert.Eventually(t, func() bool { return counter.Load() == int64(size+3) }, 3*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 4, packs[size+1].segmentID)

}

func TestRendezvousFlushManager_getSegmentMeta(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	channel := newTestChannel()
	channel.collSchema = &schemapb.CollectionSchema{}
	fm := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, channel, func(*segmentFlushPack) {
	}, emptyFlushAndDropFunc)

	// non exists segment
	_, _, _, err := fm.getSegmentMeta(-1, &msgpb.MsgPosition{})
	assert.Error(t, err)

	seg0 := Segment{segmentID: -1}
	seg1 := Segment{segmentID: 1}
	seg0.setType(datapb.SegmentType_New)
	seg1.setType(datapb.SegmentType_New)

	channel.segments[-1] = &seg0
	channel.segments[1] = &seg1

	// // injected get part/coll id error
	// _, _, _, err = fm.getSegmentMeta(-1, &msgpb.MsgPosition{})
	// assert.Error(t, err)
	// // injected get schema  error
	// _, _, _, err = fm.getSegmentMeta(1, &msgpb.MsgPosition{})
	// assert.Error(t, err)
}

func TestRendezvousFlushManager_waitForAllFlushQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	channel := newTestChannel()
	testSeg := &Segment{
		collectionID: 1,
		segmentID:    1,
	}
	testSeg.setType(datapb.SegmentType_New)
	channel.segments[testSeg.segmentID] = testSeg

	size := 1000
	var counter atomic.Int64
	m := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, channel, func(pack *segmentFlushPack) {
		counter.Inc()
	}, emptyFlushAndDropFunc)

	ids := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		id := make([]byte, 10)
		rand.Read(id)
		ids = append(ids, id)
	}

	for i := 0; i < size; i++ {
		err := m.flushDelData(nil, 1, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)
	}

	var finished bool
	var mut sync.RWMutex
	signal := make(chan struct{})

	go func() {
		m.waitForAllFlushQueue()
		mut.Lock()
		finished = true
		mut.Unlock()
		close(signal)
	}()

	mut.RLock()
	assert.False(t, finished)
	mut.RUnlock()

	for i := 0; i < size/2; i++ {
		_, err := m.flushBufferData(nil, 1, true, false, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)
	}

	mut.RLock()
	assert.False(t, finished)
	mut.RUnlock()

	for i := size / 2; i < size; i++ {
		_, err := m.flushBufferData(nil, 1, true, false, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)
	}

	select {
	case <-time.After(time.Second):
		t.FailNow()
	case <-signal:
	}

	mut.RLock()
	assert.True(t, finished)
	mut.RUnlock()
}

func TestRendezvousFlushManager_dropMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("test drop mode", func(t *testing.T) {
		cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
		defer cm.RemoveWithPrefix(ctx, cm.RootPath())

		var mut sync.Mutex
		var result []*segmentFlushPack
		signal := make(chan struct{})

		channel := newTestChannel()
		targets := make(map[int64]struct{})
		//init failed segment
		testSeg := &Segment{
			collectionID: 1,
			segmentID:    -1,
		}
		testSeg.setType(datapb.SegmentType_New)
		channel.segments[testSeg.segmentID] = testSeg

		//init target segment
		for i := 1; i < 11; i++ {
			targets[int64(i)] = struct{}{}
			testSeg := &Segment{
				collectionID: 1,
				segmentID:    int64(i),
			}
			testSeg.setType(datapb.SegmentType_New)
			channel.segments[testSeg.segmentID] = testSeg
		}

		//init flush manager
		m := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, channel, func(pack *segmentFlushPack) {
		}, func(packs []*segmentFlushPack) {
			mut.Lock()
			result = packs
			mut.Unlock()
			close(signal)
		})

		halfMsgID := []byte{1, 1, 1}
		_, err := m.flushBufferData(nil, -1, true, false, &msgpb.MsgPosition{
			MsgID: halfMsgID,
		})
		assert.NoError(t, err)

		m.startDropping()
		// half normal, half drop mode, should not appear in final packs
		err = m.flushDelData(nil, -1, &msgpb.MsgPosition{
			MsgID: halfMsgID,
		})
		assert.NoError(t, err)

		for target := range targets {
			_, err := m.flushBufferData(nil, target, true, false, &msgpb.MsgPosition{
				MsgID: []byte{byte(target)},
			})
			assert.NoError(t, err)

			err = m.flushDelData(nil, target, &msgpb.MsgPosition{
				MsgID: []byte{byte(target)},
			})
			assert.NoError(t, err)
		}

		m.notifyAllFlushed()

		<-signal
		mut.Lock()
		defer mut.Unlock()

		output := make(map[int64]struct{})
		for _, pack := range result {
			assert.NotEqual(t, -1, pack.segmentID)
			output[pack.segmentID] = struct{}{}
			_, has := targets[pack.segmentID]
			assert.True(t, has)
		}
		assert.Equal(t, len(targets), len(output))
	})

	t.Run("test drop mode with injection", func(t *testing.T) {
		cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
		defer cm.RemoveWithPrefix(ctx, cm.RootPath())

		var mut sync.Mutex
		var result []*segmentFlushPack
		signal := make(chan struct{})
		channel := newTestChannel()
		//init failed segment
		testSeg := &Segment{
			collectionID: 1,
			segmentID:    -1,
		}
		testSeg.setType(datapb.SegmentType_New)
		channel.segments[testSeg.segmentID] = testSeg

		//init target segment
		for i := 1; i < 11; i++ {
			seg := &Segment{
				collectionID: 1,
				segmentID:    int64(i),
			}
			seg.setType(datapb.SegmentType_New)
			channel.segments[seg.segmentID] = seg
		}

		m := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, channel, func(pack *segmentFlushPack) {
		}, func(packs []*segmentFlushPack) {
			mut.Lock()
			result = packs
			mut.Unlock()
			close(signal)
		})

		//flush failed segment before start drop mode
		halfMsgID := []byte{1, 1, 1}
		_, err := m.flushBufferData(nil, -1, true, false, &msgpb.MsgPosition{
			MsgID: halfMsgID,
		})
		assert.NoError(t, err)

		//inject target segment
		injFunc := func(pack *segmentFlushPack) {
			pack.segmentID = 100
		}
		for i := 1; i < 11; i++ {
			it := newTaskInjection(1, injFunc)
			m.injectFlush(it, int64(i))
			<-it.Injected()
			it.injectDone(true)
		}

		m.startDropping()
		// half normal, half drop mode, should not appear in final packs
		err = m.flushDelData(nil, -1, &msgpb.MsgPosition{
			MsgID: halfMsgID,
		})
		assert.NoError(t, err)

		for i := 1; i < 11; i++ {
			_, err = m.flushBufferData(nil, int64(i), true, false, &msgpb.MsgPosition{
				MsgID: []byte{byte(i)},
			})
			assert.NoError(t, err)

			err = m.flushDelData(nil, int64(i), &msgpb.MsgPosition{
				MsgID: []byte{byte(i)},
			})
			assert.NoError(t, err)
		}

		m.notifyAllFlushed()
		mut.Lock()
		defer mut.Unlock()

		for _, pack := range result {
			assert.NotEqual(t, -1, pack.segmentID)
			assert.Equal(t, int64(100), pack.segmentID)
		}
	})
}

func TestRendezvousFlushManager_close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	size := 1000
	var counter atomic.Int64

	channel := newTestChannel()

	//init test segment
	testSeg := &Segment{
		collectionID: 1,
		segmentID:    1,
	}
	testSeg.setType(datapb.SegmentType_New)
	channel.segments[testSeg.segmentID] = testSeg

	m := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, channel, func(pack *segmentFlushPack) {
		counter.Inc()
	}, emptyFlushAndDropFunc)

	ids := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		id := make([]byte, 10)
		rand.Read(id)
		ids = append(ids, id)
	}

	for i := 0; i < size; i++ {
		err := m.flushDelData(nil, 1, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)

		_, err = m.flushBufferData(nil, 1, true, false, &msgpb.MsgPosition{
			MsgID: ids[i],
		})
		assert.NoError(t, err)
	}

	m.close()
	assert.Eventually(t, func() bool { return counter.Load() == int64(size) }, 3*time.Second, 100*time.Millisecond)
}

func TestFlushNotifyFunc(t *testing.T) {
	rcf := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	channel := newChannel("channel", 1, nil, rcf, cm)

	dataCoord := &DataCoordFactory{}
	flushingCache := newCache()
	dsService := &dataSyncService{
		collectionID:     1,
		channel:          channel,
		dataCoord:        dataCoord,
		flushingSegCache: flushingCache,
	}
	notifyFunc := flushNotifyFunc(dsService, retry.Attempts(1))

	t.Run("normal run", func(t *testing.T) {
		assert.NotPanics(t, func() {
			notifyFunc(&segmentFlushPack{
				insertLogs: map[UniqueID]*datapb.Binlog{1: {LogPath: "/dev/test/id"}},
				statsLogs:  map[UniqueID]*datapb.Binlog{1: {LogPath: "/dev/test/id-stats"}},
				deltaLogs:  []*datapb.Binlog{{LogPath: "/dev/test/del"}},
				flushed:    true,
			})
		})
	})

	t.Run("pack has error", func(t *testing.T) {
		assert.Panics(t, func() {
			notifyFunc(&segmentFlushPack{
				err: errors.New("mocked pack error"),
			})
		})
	})

	t.Run("datacoord save fails", func(t *testing.T) {
		dataCoord.SaveBinlogPathStatus = commonpb.ErrorCode_UnexpectedError
		assert.Panics(t, func() {
			notifyFunc(&segmentFlushPack{})
		})
	})
	t.Run("normal segment not found", func(t *testing.T) {
		dataCoord.SaveBinlogPathStatus = commonpb.ErrorCode_SegmentNotFound
		assert.Panics(t, func() {
			notifyFunc(&segmentFlushPack{flushed: true})
		})
	})

	// issue https://github.com/milvus-io/milvus/issues/17097
	// meta error, datanode shall not panic, just drop the virtual channel
	t.Run("datacoord found meta error", func(t *testing.T) {
		dataCoord.SaveBinlogPathStatus = commonpb.ErrorCode_MetaFailed
		assert.NotPanics(t, func() {
			notifyFunc(&segmentFlushPack{})
		})
	})

	t.Run("datacoord call error", func(t *testing.T) {
		dataCoord.SaveBinlogPathError = true
		assert.Panics(t, func() {
			notifyFunc(&segmentFlushPack{})
		})
	})
}

func TestDropVirtualChannelFunc(t *testing.T) {
	rcf := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	vchanName := "vchan_01"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(flushTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	channel := newChannel(vchanName, 1, nil, rcf, cm)

	dataCoord := &DataCoordFactory{}
	flushingCache := newCache()
	dsService := &dataSyncService{
		collectionID:     1,
		channel:          channel,
		dataCoord:        dataCoord,
		flushingSegCache: flushingCache,
		vchannelName:     vchanName,
	}
	dropFunc := dropVirtualChannelFunc(dsService, retry.Attempts(1))
	t.Run("normal run", func(t *testing.T) {
		channel.addSegment(
			addSegmentReq{
				segType:     datapb.SegmentType_New,
				segID:       2,
				collID:      1,
				partitionID: 10,
				startPos: &msgpb.MsgPosition{
					ChannelName: vchanName,
					MsgID:       []byte{1, 2, 3},
					Timestamp:   10,
				}, endPos: nil})
		assert.NotPanics(t, func() {
			dropFunc([]*segmentFlushPack{
				{
					segmentID:  1,
					insertLogs: map[UniqueID]*datapb.Binlog{1: {LogPath: "/dev/test/id"}},
					statsLogs:  map[UniqueID]*datapb.Binlog{1: {LogPath: "/dev/test/id-stats"}},
					deltaLogs:  []*datapb.Binlog{{LogPath: "/dev/test/del"}},
					pos: &msgpb.MsgPosition{
						ChannelName: vchanName,
						MsgID:       []byte{1, 2, 3},
						Timestamp:   10,
					},
				},
				{
					segmentID:  1,
					insertLogs: map[UniqueID]*datapb.Binlog{1: {LogPath: "/dev/test/idi_2"}},
					statsLogs:  map[UniqueID]*datapb.Binlog{1: {LogPath: "/dev/test/id-stats-2"}},
					deltaLogs:  []*datapb.Binlog{{LogPath: "/dev/test/del-2"}},
					pos: &msgpb.MsgPosition{
						ChannelName: vchanName,
						MsgID:       []byte{1, 2, 3},
						Timestamp:   30,
					},
				},
			})
		})
	})
	t.Run("datacoord drop fails", func(t *testing.T) {
		dataCoord.DropVirtualChannelStatus = commonpb.ErrorCode_UnexpectedError
		assert.Panics(t, func() {
			dropFunc(nil)
		})
	})

	t.Run("datacoord call error", func(t *testing.T) {

		dataCoord.DropVirtualChannelStatus = commonpb.ErrorCode_UnexpectedError
		dataCoord.DropVirtualChannelError = true
		assert.Panics(t, func() {
			dropFunc(nil)
		})
	})

	// issue https://github.com/milvus-io/milvus/issues/17097
	// meta error, datanode shall not panic, just drop the virtual channel
	t.Run("datacoord found meta error", func(t *testing.T) {
		dataCoord.DropVirtualChannelStatus = commonpb.ErrorCode_MetaFailed
		dataCoord.DropVirtualChannelError = false
		assert.NotPanics(t, func() {
			dropFunc(nil)
		})
	})

}
