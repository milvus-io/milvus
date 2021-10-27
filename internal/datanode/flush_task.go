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
	"errors"
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
//  then call the notifyFunc
type flushTaskRunner struct {
	sync.WaitGroup
	kv.BaseKV

	initOnce   sync.Once
	insertOnce sync.Once
	deleteOnce sync.Once

	startSignal  <-chan struct{}
	finishSignal chan struct{}
	injectSignal <-chan taskInjection

	segmentID  UniqueID
	insertLogs map[UniqueID]string
	statsLogs  map[UniqueID]string
	deltaLogs  []*DelDataBuf
	pos        *internalpb.MsgPosition
	flushed    bool
}

type taskInjection struct {
	injected      chan struct{} // channel to notify injected
	injectOver    chan bool     // indicates injection over
	postInjection func(pack *segmentFlushPack)
}

// init initializes flushTaskRunner with provided actions and signal
func (t *flushTaskRunner) init(f notifyMetaFunc, postFunc taskPostFunc, signal <-chan struct{}) {
	t.initOnce.Do(func() {
		t.startSignal = signal
		t.finishSignal = make(chan struct{})
		go t.waitFinish(f, postFunc)
	})
}

// runFlushInsert executei flush insert task with once and retry
func (t *flushTaskRunner) runFlushInsert(task flushInsertTask, binlogs, statslogs map[UniqueID]string, flushed bool, pos *internalpb.MsgPosition) {
	t.insertOnce.Do(func() {
		t.insertLogs = binlogs
		t.statsLogs = statslogs
		t.flushed = flushed
		t.pos = pos
		go func() {
			err := errStart
			for err != nil {
				err = task.flushInsertData()
			}
			t.Done()
		}()
	})
}

// runFlushDel execute flush delete task with once and retry
func (t *flushTaskRunner) runFlushDel(task flushDeleteTask, deltaLogs *DelDataBuf) {
	t.deleteOnce.Do(func() {
		if deltaLogs == nil {
			t.deltaLogs = []*DelDataBuf{}
		} else {
			t.deltaLogs = []*DelDataBuf{deltaLogs}
		}
		go func() {
			err := errStart
			for err != nil {
				err = task.flushDeleteData()
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
	var postInjection postInjectionFunc = nil
	select {
	case injection := <-t.injectSignal:
		// notify injected
		injection.injected <- struct{}{}
		ok := <-injection.injectOver
		if ok {
			// apply postInjection func
			postInjection = injection.postInjection
		}
	default:
	}
	postFunc(pack, postInjection)

	// execution done, dequeue and make count --
	err := errStart
	for err != nil {
		err = notifyFunc(pack)
	}

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
	}

	return pack
}

// newFlushTaskRunner create a usable task runner
func newFlushTaskRunner(segmentID UniqueID, injectCh <-chan taskInjection) *flushTaskRunner {
	t := &flushTaskRunner{
		WaitGroup:    sync.WaitGroup{},
		segmentID:    segmentID,
		injectSignal: injectCh,
	}
	// insert & del
	t.Add(2)
	return t
}
