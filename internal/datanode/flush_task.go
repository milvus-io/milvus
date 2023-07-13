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
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// errStart used for retry start
var errStart = errors.New("start")

// flushInsertTask defines action for flush insert
type flushInsertTask interface {
	flushInsertData() error
}

// flushDeleteTask defines action for flush delete
type flushDeleteTask interface {
	flushDeleteData() error
}

// flushTaskRunner controls a single flush task lifetime
// this runner will wait insert data flush & del data flush done
//
//	then call the notifyFunc
type flushTaskRunner struct {
	sync.WaitGroup
	kv.BaseKV

	initOnce   sync.Once
	insertOnce sync.Once
	deleteOnce sync.Once

	startSignal  <-chan struct{}
	finishSignal chan struct{}
	injectSignal <-chan *taskInjection

	segmentID  UniqueID
	insertLogs map[UniqueID]*datapb.Binlog
	statsLogs  map[UniqueID]*datapb.Binlog
	deltaLogs  []*datapb.Binlog //[]*DelDataBuf
	pos        *msgpb.MsgPosition
	flushed    bool
	dropped    bool

	insertErr error // task execution error
	deleteErr error // task execution error
}

type taskInjection struct {
	injected      chan struct{} // channel to notify injected
	injectOver    chan bool     // indicates injection over
	wg            sync.WaitGroup
	postInjection func(pack *segmentFlushPack)
}

func newTaskInjection(segmentCnt int, pf func(pack *segmentFlushPack)) *taskInjection {
	ti := &taskInjection{
		injected:      make(chan struct{}),
		injectOver:    make(chan bool, segmentCnt),
		postInjection: pf,
	}
	ti.wg.Add(segmentCnt)
	return ti
}

// Injected returns a chan, which will be closed after pre set segments counts an injected
func (ti *taskInjection) Injected() <-chan struct{} {
	return ti.injected
}

func (ti *taskInjection) waitForInjected() {
	ti.wg.Wait()
	close(ti.injected)
}

func (ti *taskInjection) injectOne() {
	ti.wg.Done()
}

func (ti *taskInjection) injectDone(success bool) {
	if !success {
		close(ti.injectOver)
		return
	}

	for i := 0; i < cap(ti.injectOver); i++ {
		ti.injectOver <- true
	}
}

// init initializes flushTaskRunner with provided actions and signal
func (t *flushTaskRunner) init(f notifyMetaFunc, postFunc taskPostFunc, signal <-chan struct{}) {
	t.initOnce.Do(func() {
		t.startSignal = signal
		t.finishSignal = make(chan struct{})
		go t.waitFinish(f, postFunc)
	})
}

// runFlushInsert executes flush insert task with once and retry
func (t *flushTaskRunner) runFlushInsert(task flushInsertTask,
	binlogs, statslogs map[UniqueID]*datapb.Binlog, flushed bool, dropped bool, pos *msgpb.MsgPosition, opts ...retry.Option) {
	t.insertOnce.Do(func() {
		t.insertLogs = binlogs
		t.statsLogs = statslogs
		t.flushed = flushed
		t.pos = pos
		t.dropped = dropped
		log.Info("running flush insert task",
			zap.Int64("segmentID", t.segmentID),
			zap.Bool("flushed", flushed),
			zap.Bool("dropped", dropped),
			zap.Any("position", pos),
			zap.Time("PosTime", tsoutil.PhysicalTime(pos.GetTimestamp())),
		)
		go func() {
			err := retry.Do(context.Background(), func() error {
				return task.flushInsertData()
			}, opts...)
			if err != nil {
				t.insertErr = err
			}
			t.Done()
		}()
	})
}

// runFlushDel execute flush delete task with once and retry
func (t *flushTaskRunner) runFlushDel(task flushDeleteTask, deltaLogs *DelDataBuf, opts ...retry.Option) {
	t.deleteOnce.Do(func() {
		if deltaLogs == nil {
			t.deltaLogs = nil //[]*DelDataBuf{}
		} else {
			t.deltaLogs = []*datapb.Binlog{
				{
					LogSize:       deltaLogs.GetLogSize(),
					LogPath:       deltaLogs.GetLogPath(),
					TimestampFrom: deltaLogs.GetTimestampFrom(),
					TimestampTo:   deltaLogs.GetTimestampTo(),
					EntriesNum:    deltaLogs.GetEntriesNum(),
				},
			}
		}
		go func() {
			err := retry.Do(context.Background(), func() error {
				return task.flushDeleteData()
			}, opts...)
			if err != nil {
				t.deleteErr = err
			}
			t.Done()
		}()
	})
}

// waitFinish waits flush & insert done
func (t *flushTaskRunner) waitFinish(notifyFunc notifyMetaFunc, postFunc taskPostFunc) {
	// wait insert & del done
	t.Wait()
	// wait previous task done
	<-t.startSignal

	pack := t.getFlushPack()
	var postInjection postInjectionFunc
	select {
	case injection := <-t.injectSignal:
		// notify injected
		injection.injectOne()
		ok := <-injection.injectOver
		if ok {
			// apply postInjection func
			postInjection = injection.postInjection
		}
	default:
	}
	postFunc(pack, postInjection)

	// execution done, dequeue and make count --
	notifyFunc(pack)

	// notify next task
	close(t.finishSignal)
}

func (t *flushTaskRunner) getFlushPack() *segmentFlushPack {
	pack := &segmentFlushPack{
		segmentID:  t.segmentID,
		insertLogs: t.insertLogs,
		statsLogs:  t.statsLogs,
		pos:        t.pos,
		deltaLogs:  t.deltaLogs,
		flushed:    t.flushed,
		dropped:    t.dropped,
	}
	log.Debug("flush pack composed",
		zap.Int64("segmentID", t.segmentID),
		zap.Int("insertLogs", len(t.insertLogs)),
		zap.Int("statsLogs", len(t.statsLogs)),
		zap.Int("deleteLogs", len(t.deltaLogs)),
		zap.Bool("flushed", t.flushed),
		zap.Bool("dropped", t.dropped),
	)

	if t.insertErr != nil || t.deleteErr != nil {
		log.Warn("flush task error detected", zap.Error(t.insertErr), zap.Error(t.deleteErr))
		pack.err = errors.New("execution failed")
	}

	return pack
}

// newFlushTaskRunner create a usable task runner
func newFlushTaskRunner(segmentID UniqueID, injectCh <-chan *taskInjection) *flushTaskRunner {
	t := &flushTaskRunner{
		WaitGroup:    sync.WaitGroup{},
		segmentID:    segmentID,
		injectSignal: injectCh,
	}
	// insert & del
	t.Add(2)
	return t
}
