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
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ChannelManager struct {
	dn *DataNode

	communicateCh     chan opState
	runningFlowgraphs *flowgraphManager
	opRunners         *typeutil.ConcurrentMap[string, *opRunner]

	closeCh     chan struct{}
	closeOnce   sync.Once
	closeWaiter sync.WaitGroup
}

func NewChannelManager(dn *DataNode) *ChannelManager {
	return &ChannelManager{
		dn: dn,

		communicateCh:     make(chan opState, 100),
		runningFlowgraphs: newFlowgraphManager(),
		opRunners:         typeutil.NewConcurrentMap[string, *opRunner](),

		closeCh: make(chan struct{}),
	}
}

func (m *ChannelManager) Start() {
	m.closeWaiter.Add(2)

	go m.runningFlowgraphs.start(&m.closeWaiter)
	go func() {
		defer m.closeWaiter.Done()
		log.Info("DataNode ChannelManager start")
		for {
			select {
			case opState := <-m.communicateCh:
				m.handleOpState(opState)
			case <-m.closeCh:
				log.Info("DataNode ChannelManager exit")
				return
			}
		}
	}()
}

func (m *ChannelManager) handleOpState(opState opState) {
	switch opState.state {
	case datapb.ChannelWatchState_WatchSuccess:
		m.runningFlowgraphs.Add(opState.fg)
		m.getOrCreateRunner(opState.channel).FinishOp(opState.opID)
	case datapb.ChannelWatchState_WatchFailure:
		m.getOrCreateRunner(opState.channel).FinishOp(opState.opID)
	case datapb.ChannelWatchState_ToRelease:
		// release should be unblocking quick return func
		m.runningFlowgraphs.release(opState.channel)
		m.destoryRunner(opState.channel)
	}
}

func (m *ChannelManager) Close() {
	m.closeOnce.Do(func() {
		m.opRunners.Range(func(channel string, runner *opRunner) bool {
			runner.Close()
			return true
		})
		m.runningFlowgraphs.close()
		close(m.closeCh)
		m.closeWaiter.Wait()
	})
}

func (m *ChannelManager) getOrCreateRunner(channel string) *opRunner {
	runner, loaded := m.opRunners.GetOrInsert(channel, NewOpRunner(channel, m.dn, m.communicateCh))
	if !loaded {
		runner.Start()
	}
	return runner
}

func (m *ChannelManager) destoryRunner(channel string) {
	if runner, loaded := m.opRunners.GetAndRemove(channel); loaded {
		runner.Close()
	}
}

func (m *ChannelManager) Submit(info *datapb.ChannelWatchInfo) error {
	channel := info.GetVchan().GetChannelName()
	runner := m.getOrCreateRunner(channel)
	return runner.Enqueue(info)
}

func (c *ChannelManager) GetProgress(info *datapb.ChannelWatchInfo) *datapb.ChannelOperationProgressResponse {
	result := &datapb.ChannelOperationProgressResponse{
		OpID: info.GetOpID(),
	}

	channel := info.GetVchan().GetChannelName()
	switch info.GetState() {
	case datapb.ChannelWatchState_ToWatch:
		if c.runningFlowgraphs.exist(channel) {
			result.State = datapb.ChannelWatchState_WatchSuccess
			result.Progress = 100
			return result
		}
		if runner, ok := c.opRunners.Get(channel); ok {
			result.Progress = runner.GetOpProgress(info.GetOpID())
			if runner.Exist(info.GetOpID()) {
				result.State = datapb.ChannelWatchState_ToWatch
			} else {
				result.State = datapb.ChannelWatchState_WatchFailure
			}
			return result
		}

		result.State = datapb.ChannelWatchState_WatchFailure
		return result

	case datapb.ChannelWatchState_ToRelease:
		if !c.runningFlowgraphs.exist(channel) {
			result.State = datapb.ChannelWatchState_ReleaseSuccess
			result.Progress = 100
			return result
		}
		if runner, ok := c.opRunners.Get(channel); ok && runner.Exist(info.GetOpID()) {
			result.State = datapb.ChannelWatchState_ToRelease
			result.Progress = runner.GetOpProgress(info.GetOpID())

			return result
		}
		result.State = datapb.ChannelWatchState_ReleaseFailure
		return result
	default:
		log.Warn("Invalid channel watch state", zap.String("state", info.GetState().String()))
		return nil
	}
}

type opRunner struct {
	channel string
	dn      *DataNode

	guard      sync.RWMutex
	allOps     map[UniqueID]*tickler // opID -> tickler
	opsInQueue chan *datapb.ChannelWatchInfo
	resultCh   chan opState

	closeWg   sync.WaitGroup
	closeOnce sync.Once
	closeCh   chan struct{}
}

func NewOpRunner(channel string, dn *DataNode, resultCh chan opState) *opRunner {
	return &opRunner{
		channel:    channel,
		dn:         dn,
		opsInQueue: make(chan *datapb.ChannelWatchInfo, 10),
		allOps:     make(map[UniqueID]*tickler),
		resultCh:   resultCh,
		closeCh:    make(chan struct{}),
	}
}

func (r *opRunner) Start() {
	r.closeWg.Add(1)
	go func() {
		defer r.closeWg.Done()
		for {
			select {
			case info := <-r.opsInQueue:
				r.NotifyState(r.Execute(info))
			case <-r.closeCh:
				return
			}
		}
	}()
}

func (r *opRunner) FinishOp(opID UniqueID) {
	r.guard.Lock()
	defer r.guard.Unlock()
	delete(r.allOps, opID)
}

func (r *opRunner) Exist(opID UniqueID) bool {
	r.guard.Lock()
	defer r.guard.Unlock()
	_, ok := r.allOps[opID]
	return ok
}

func (r *opRunner) GetOpProgress(opID UniqueID) int32 {
	r.guard.RLock()
	defer r.guard.RUnlock()
	if tickler, ok := r.allOps[opID]; ok {
		return tickler.progress()
	}

	return 100
}

// Execute excutes channel operations, channel state is validated during enqueue
func (r *opRunner) Execute(info *datapb.ChannelWatchInfo) opState {
	opState := opState{
		channel: info.GetVchan().GetChannelName(),
		opID:    info.GetOpID(),
	}

	if info.GetState() == datapb.ChannelWatchState_ToWatch {
		r.guard.RLock()
		tickler, ok := r.allOps[info.GetOpID()]
		r.guard.RUnlock()
		if !ok {
			opState.state = datapb.ChannelWatchState_WatchFailure
			return opState
		}

		fg, err := ExecuteWatchOperation(r.dn, info, tickler)
		if err != nil {
			opState.state = datapb.ChannelWatchState_WatchFailure
			return opState
		}

		opState.state = datapb.ChannelWatchState_WatchSuccess
		opState.fg = fg
		return opState
	}

	opState.state = datapb.ChannelWatchState_ToRelease
	return opState
}

// WatchOperation will always return, won't be stuck, either success or fail.
// TODO wait for ctx timeout error
func ExecuteWatchOperation(dn *DataNode, info *datapb.ChannelWatchInfo, tickler *tickler) (*dataSyncService, error) {
	log.Info("Execute channel watch operation", zap.String("channel", info.GetVchan().GetChannelName()))

	dataSyncService := getDataSyncService(dn, info, tickler)
	if dataSyncService != nil {
		dataSyncService.start()
	}

	return dataSyncService, nil
}

func (r *opRunner) Enqueue(info *datapb.ChannelWatchInfo) error {
	if info.GetState() != datapb.ChannelWatchState_ToWatch &&
		info.GetState() != datapb.ChannelWatchState_ToRelease {
		return errors.New("Invalid channel watch state")
	}

	r.guard.Lock()
	if _, ok := r.allOps[info.GetOpID()]; !ok {
		r.opsInQueue <- info
		r.allOps[info.GetOpID()] = newTickler()
	}
	r.guard.Unlock()
	return nil
}

func (r *opRunner) NotifyState(state opState) {
	r.resultCh <- state
}

func (r *opRunner) Close() {
	r.guard.Lock()
	for _, tickler := range r.allOps {
		tickler.close()
	}
	r.guard.Unlock()

	r.closeOnce.Do(func() {
		close(r.closeCh)
		r.closeWg.Wait()
	})
}

type opState struct {
	channel string
	opID    int64
	state   datapb.ChannelWatchState
	fg      *dataSyncService
}

func getDataSyncService(node *DataNode, info *datapb.ChannelWatchInfo, tickler *tickler) *dataSyncService {
	channelName := info.GetVchan().GetChannelName()
	log := log.With(zap.String("channel", channelName))

	channel := newChannel(
		info.GetVchan().GetChannelName(),
		info.GetVchan().GetCollectionID(),
		info.GetSchema(),
		node.rootCoord,
		node.chunkManager,
	)

	dataSyncService, err := newDataSyncService(
		node.ctx,
		make(chan flushMsg, 100),
		make(chan resendTTMsg, 100),
		channel,
		node.allocator,
		node.dispClient,
		node.factory,
		info.GetVchan(),
		node.clearSignal,
		node.dataCoord,
		node.segmentCache,
		node.chunkManager,
		node.compactionExecutor,
		tickler,
		node.GetSession().ServerID,
		node.timeTickSender,
	)
	if err != nil {
		log.Warn("fail to create new datasyncservice", zap.Error(err))
		return nil
	}

	return dataSyncService
}

func (m *ChannelManager) Release(channel string) {
	log.Info("try to release flowgraph", zap.String("channel", channel))
	m.runningFlowgraphs.release(channel)
}

// tickler counts every time when called inc(),
type tickler struct {
	count *atomic.Int32
	total *atomic.Int32
	sig   *atomic.Bool
}

func (t *tickler) inc() {
	t.count.Inc()
}

func (t *tickler) setTotal(total int32) {
	t.total.Store(total)
}

// progress returns the count over total if total is set
// else just return the count number.
func (t *tickler) progress() int32 {
	if t.total.Load() == 0 {
		return t.count.Load()
	}
	return (t.count.Load() / t.total.Load()) * 100
}

func (t *tickler) close() {
	t.sig.CompareAndSwap(false, true)
}

func (t *tickler) closed() bool {
	return t.sig.Load()
}

func newTickler() *tickler {
	return &tickler{
		count: atomic.NewInt32(0),
		total: atomic.NewInt32(0),
		sig:   atomic.NewBool(false),
	}
}
