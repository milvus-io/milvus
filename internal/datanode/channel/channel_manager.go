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

package channel

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type (
	releaseFunc func(channel string)
	watchFunc   func(ctx context.Context, pipelineParams *util.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util.Tickler) (*pipeline.DataSyncService, error)
)

type ChannelManager interface {
	Submit(info *datapb.ChannelWatchInfo) error
	GetProgress(info *datapb.ChannelWatchInfo) *datapb.ChannelOperationProgressResponse
	Close()
	Start()
}

type ChannelManagerImpl struct {
	mu             sync.RWMutex
	pipelineParams *util.PipelineParams

	fgManager pipeline.FlowgraphManager

	communicateCh chan *opState
	opRunners     *typeutil.ConcurrentMap[string, *opRunner] // channel -> runner
	abnormals     *typeutil.ConcurrentMap[int64, string]     // OpID -> Channel

	releaseFunc releaseFunc

	closeCh     lifetime.SafeChan
	closeWaiter sync.WaitGroup
}

func NewChannelManager(pipelineParams *util.PipelineParams, fgManager pipeline.FlowgraphManager) *ChannelManagerImpl {
	cm := ChannelManagerImpl{
		pipelineParams: pipelineParams,
		fgManager:      fgManager,

		communicateCh: make(chan *opState, 100),
		opRunners:     typeutil.NewConcurrentMap[string, *opRunner](),
		abnormals:     typeutil.NewConcurrentMap[int64, string](),

		releaseFunc: fgManager.RemoveFlowgraph,

		closeCh: lifetime.NewSafeChan(),
	}

	return &cm
}

func (m *ChannelManagerImpl) Submit(info *datapb.ChannelWatchInfo) error {
	channel := info.GetVchan().GetChannelName()

	// skip enqueue datacoord re-submit the same operations
	if runner, ok := m.opRunners.Get(channel); ok {
		if runner.Exist(info.GetOpID()) {
			log.Warn("op already exist, skip", zap.Int64("opID", info.GetOpID()), zap.String("channel", channel))
			return nil
		}
	}

	if info.GetState() == datapb.ChannelWatchState_ToWatch &&
		m.fgManager.HasFlowgraphWithOpID(channel, info.GetOpID()) {
		log.Warn("Watch op already finished, skip", zap.Int64("opID", info.GetOpID()), zap.String("channel", channel))
		return nil
	}

	if info.GetState() == datapb.ChannelWatchState_ToRelease &&
		!m.fgManager.HasFlowgraph(channel) {
		log.Warn("Release op already finished, skip", zap.Int64("opID", info.GetOpID()), zap.String("channel", channel))
		return nil
	}

	runner := m.getOrCreateRunner(channel)
	return runner.Enqueue(info)
}

func (m *ChannelManagerImpl) GetProgress(info *datapb.ChannelWatchInfo) *datapb.ChannelOperationProgressResponse {
	m.mu.RLock()
	defer m.mu.RUnlock()
	resp := &datapb.ChannelOperationProgressResponse{
		Status: merr.Success(),
		OpID:   info.GetOpID(),
	}

	channel := info.GetVchan().GetChannelName()
	switch info.GetState() {
	case datapb.ChannelWatchState_ToWatch:
		// running flowgraph means watch success
		if m.fgManager.HasFlowgraphWithOpID(channel, info.GetOpID()) {
			resp.State = datapb.ChannelWatchState_WatchSuccess
			resp.Progress = 100
			return resp
		}

		if runner, ok := m.opRunners.Get(channel); ok {
			if runner.Exist(info.GetOpID()) {
				resp.State = datapb.ChannelWatchState_ToWatch
			} else {
				resp.State = datapb.ChannelWatchState_WatchFailure
			}
			return resp
		}
		resp.State = datapb.ChannelWatchState_WatchFailure
		return resp

	case datapb.ChannelWatchState_ToRelease:
		if !m.fgManager.HasFlowgraph(channel) {
			resp.State = datapb.ChannelWatchState_ReleaseSuccess
			return resp
		}
		if runner, ok := m.opRunners.Get(channel); ok && runner.Exist(info.GetOpID()) {
			resp.State = datapb.ChannelWatchState_ToRelease
			return resp
		}

		resp.State = datapb.ChannelWatchState_ReleaseFailure
		return resp
	default:
		err := merr.WrapErrParameterInvalid("ToWatch or ToRelease", info.GetState().String())
		log.Warn("fail to get progress", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp
	}
}

func (m *ChannelManagerImpl) Close() {
	if m.opRunners != nil {
		m.opRunners.Range(func(channel string, runner *opRunner) bool {
			runner.Close()
			return true
		})
	}
	m.closeCh.Close()
	m.closeWaiter.Wait()
}

func (m *ChannelManagerImpl) Start() {
	m.closeWaiter.Add(1)
	go func() {
		defer m.closeWaiter.Done()
		log.Info("DataNode ChannelManager start")
		for {
			select {
			case opState := <-m.communicateCh:
				m.handleOpState(opState)
			case <-m.closeCh.CloseCh():
				log.Info("DataNode ChannelManager exit")
				return
			}
		}
	}()
}

func (m *ChannelManagerImpl) handleOpState(opState *opState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	log := log.With(
		zap.Int64("opID", opState.opID),
		zap.String("channel", opState.channel),
		zap.String("State", opState.state.String()),
	)
	switch opState.state {
	case datapb.ChannelWatchState_WatchSuccess:
		log.Info("Success to watch")
		m.fgManager.AddFlowgraph(opState.fg)

	case datapb.ChannelWatchState_WatchFailure:
		log.Info("Fail to watch")

	case datapb.ChannelWatchState_ReleaseSuccess:
		log.Info("Success to release")

	case datapb.ChannelWatchState_ReleaseFailure:
		log.Info("Fail to release, add channel to abnormal lists")
		m.abnormals.Insert(opState.opID, opState.channel)
	}

	m.finishOp(opState.opID, opState.channel)
}

func (m *ChannelManagerImpl) getOrCreateRunner(channel string) *opRunner {
	runner, loaded := m.opRunners.GetOrInsert(channel, NewOpRunner(channel, m.pipelineParams, m.releaseFunc, executeWatch, m.communicateCh))
	if !loaded {
		runner.Start()
	}
	return runner
}

func (m *ChannelManagerImpl) finishOp(opID int64, channel string) {
	if runner, loaded := m.opRunners.GetAndRemove(channel); loaded {
		runner.FinishOp(opID)
		runner.Close()
	}
}

type opInfo struct {
	tickler *util.Tickler
}

type opRunner struct {
	channel        string
	pipelineParams *util.PipelineParams
	releaseFunc    releaseFunc
	watchFunc      watchFunc

	guard      sync.RWMutex
	allOps     map[util.UniqueID]*opInfo // opID -> tickler
	opsInQueue chan *datapb.ChannelWatchInfo
	resultCh   chan *opState

	closeCh lifetime.SafeChan
	closeWg sync.WaitGroup
}

func NewOpRunner(channel string, pipelineParams *util.PipelineParams, releaseF releaseFunc, watchF watchFunc, resultCh chan *opState) *opRunner {
	return &opRunner{
		channel:        channel,
		pipelineParams: pipelineParams,
		releaseFunc:    releaseF,
		watchFunc:      watchF,
		opsInQueue:     make(chan *datapb.ChannelWatchInfo, 10),
		allOps:         make(map[util.UniqueID]*opInfo),
		resultCh:       resultCh,
		closeCh:        lifetime.NewSafeChan(),
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
			case <-r.closeCh.CloseCh():
				return
			}
		}
	}()
}

func (r *opRunner) FinishOp(opID util.UniqueID) {
	r.guard.Lock()
	defer r.guard.Unlock()
	delete(r.allOps, opID)
}

func (r *opRunner) Exist(opID util.UniqueID) bool {
	r.guard.RLock()
	defer r.guard.RUnlock()
	_, ok := r.allOps[opID]
	return ok
}

func (r *opRunner) Enqueue(info *datapb.ChannelWatchInfo) error {
	if info.GetState() != datapb.ChannelWatchState_ToWatch &&
		info.GetState() != datapb.ChannelWatchState_ToRelease {
		return errors.New("Invalid channel watch state")
	}

	r.guard.Lock()
	defer r.guard.Unlock()
	if _, ok := r.allOps[info.GetOpID()]; !ok {
		r.opsInQueue <- info
		r.allOps[info.GetOpID()] = &opInfo{}
	}
	return nil
}

func (r *opRunner) UnfinishedOpSize() int {
	r.guard.RLock()
	defer r.guard.RUnlock()
	return len(r.allOps)
}

// Execute excutes channel operations, channel state is validated during enqueue
func (r *opRunner) Execute(info *datapb.ChannelWatchInfo) *opState {
	log.Info("Start to execute channel operation",
		zap.String("channel", info.GetVchan().GetChannelName()),
		zap.Int64("opID", info.GetOpID()),
		zap.String("state", info.GetState().String()),
	)
	if info.GetState() == datapb.ChannelWatchState_ToWatch {
		return r.watchWithTimer(info)
	}

	// ToRelease state
	return r.releaseWithTimer(r.releaseFunc, info.GetVchan().GetChannelName(), info.GetOpID())
}

// watchWithTimer will return WatchFailure after WatchTimeoutInterval
func (r *opRunner) watchWithTimer(info *datapb.ChannelWatchInfo) *opState {
	opState := &opState{
		channel: info.GetVchan().GetChannelName(),
		opID:    info.GetOpID(),
	}
	log := log.With(zap.String("channel", opState.channel), zap.Int64("opID", opState.opID))

	r.guard.Lock()
	opInfo, ok := r.allOps[info.GetOpID()]
	r.guard.Unlock()
	if !ok {
		opState.state = datapb.ChannelWatchState_WatchFailure
		return opState
	}
	tickler := util.NewTickler()
	opInfo.tickler = tickler

	var (
		successSig   = make(chan struct{}, 1)
		finishWaiter sync.WaitGroup
	)

	watchTimeout := paramtable.Get().DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startTimer := func(finishWg *sync.WaitGroup) {
		defer finishWg.Done()

		timer := time.NewTimer(watchTimeout)
		defer timer.Stop()

		log := log.With(zap.Duration("timeout", watchTimeout))
		log.Info("Start timer for ToWatch operation")
		for {
			select {
			case <-timer.C:
				// watch timeout
				tickler.Close()
				cancel()
				log.Info("Stop timer for ToWatch operation timeout")
				return

			case <-r.closeCh.CloseCh():
				// runner closed from outside
				tickler.Close()
				cancel()
				log.Info("Suspend ToWatch operation from outside of opRunner")
				return

			case <-tickler.GetProgressSig():
				log.Info("Reset timer for tickler updated")
				timer.Reset(watchTimeout)

			case <-successSig:
				// watch success
				log.Info("Stop timer for ToWatch operation succeeded")
				return
			}
		}
	}

	finishWaiter.Add(2)
	go startTimer(&finishWaiter)

	go func() {
		defer finishWaiter.Done()
		fg, err := r.watchFunc(ctx, r.pipelineParams, info, tickler)
		if err != nil {
			opState.state = datapb.ChannelWatchState_WatchFailure
		} else {
			opState.state = datapb.ChannelWatchState_WatchSuccess
			opState.fg = fg
			successSig <- struct{}{}
		}
	}()

	finishWaiter.Wait()
	return opState
}

// releaseWithTimer will return ReleaseFailure after WatchTimeoutInterval
func (r *opRunner) releaseWithTimer(releaseFunc releaseFunc, channel string, opID util.UniqueID) *opState {
	opState := &opState{
		channel: channel,
		opID:    opID,
	}
	var (
		successSig   = make(chan struct{}, 1)
		finishWaiter sync.WaitGroup
	)

	log := log.With(zap.Int64("opID", opID), zap.String("channel", channel))
	startTimer := func(finishWaiter *sync.WaitGroup) {
		defer finishWaiter.Done()

		releaseTimeout := paramtable.Get().DataCoordCfg.WatchTimeoutInterval.GetAsDuration(time.Second)
		timer := time.NewTimer(releaseTimeout)
		defer timer.Stop()

		log := log.With(zap.Duration("timeout", releaseTimeout))
		log.Info("Start ToRelease timer")
		for {
			select {
			case <-timer.C:
				log.Info("Stop timer for ToRelease operation timeout")
				opState.state = datapb.ChannelWatchState_ReleaseFailure
				return

			case <-r.closeCh.CloseCh():
				// runner closed from outside
				log.Info("Stop timer for opRunner closed")
				return

			case <-successSig:
				log.Info("Stop timer for ToRelease operation succeeded")
				opState.state = datapb.ChannelWatchState_ReleaseSuccess
				return
			}
		}
	}

	finishWaiter.Add(1)
	go startTimer(&finishWaiter)
	go func() {
		// TODO: failure should panic this DN, but we're not sure how
		//   to recover when releaseFunc stuck.
		// Whenever we see a stuck, it's a bug need to be fixed.
		// In case of the unknown behavior after the stuck of release,
		//   we'll mark this channel abnormal in this DN. This goroutine might never return.
		//
		// The channel can still be balanced into other DNs, but not on this one.
		// ExclusiveConsumer error happens when the same DN subscribes the same pchannel twice.
		releaseFunc(opState.channel)
		successSig <- struct{}{}
	}()

	finishWaiter.Wait()
	return opState
}

func (r *opRunner) NotifyState(state *opState) {
	r.resultCh <- state
}

func (r *opRunner) Close() {
	r.closeCh.Close()
	r.closeWg.Wait()
}

type opState struct {
	channel string
	opID    int64
	state   datapb.ChannelWatchState
	fg      *pipeline.DataSyncService
}

// executeWatch will always return, won't be stuck, either success or fail.
func executeWatch(ctx context.Context, pipelineParams *util.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util.Tickler) (*pipeline.DataSyncService, error) {
	dataSyncService, err := pipeline.NewDataSyncService(ctx, pipelineParams, info, tickler)
	if err != nil {
		return nil, err
	}

	dataSyncService.Start()

	return dataSyncService, nil
}
