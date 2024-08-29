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
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ flusher.Flusher = (*flusherImpl)(nil)

type flusherImpl struct {
	broker    broker.Broker
	fgMgr     pipeline.FlowgraphManager
	syncMgr   syncmgr.SyncManager
	wbMgr     writebuffer.BufferManager
	cpUpdater *util.ChannelCheckpointUpdater

	tasks    *typeutil.ConcurrentMap[string, ChannelTask]
	scanners *typeutil.ConcurrentMap[string, wal.Scanner] // watched scanners

	notifyCh       chan struct{}
	stopChan       lifetime.SafeChan
	stopWg         sync.WaitGroup
	pipelineParams *util.PipelineParams
}

func NewFlusher(chunkManager storage.ChunkManager) flusher.Flusher {
	params := getPipelineParams(chunkManager)
	return newFlusherWithParam(params)
}

func newFlusherWithParam(params *util.PipelineParams) flusher.Flusher {
	fgMgr := pipeline.NewFlowgraphManager()
	return &flusherImpl{
		broker:         params.Broker,
		fgMgr:          fgMgr,
		syncMgr:        params.SyncMgr,
		wbMgr:          params.WriteBufferManager,
		cpUpdater:      params.CheckpointUpdater,
		tasks:          typeutil.NewConcurrentMap[string, ChannelTask](),
		scanners:       typeutil.NewConcurrentMap[string, wal.Scanner](),
		notifyCh:       make(chan struct{}, 1),
		stopChan:       lifetime.NewSafeChan(),
		pipelineParams: params,
	}
}

func (f *flusherImpl) RegisterPChannel(pchannel string, wal wal.WAL) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := resource.Resource().RootCoordClient().GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
		Pchannel: pchannel,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	for _, collectionInfo := range resp.GetCollections() {
		f.RegisterVChannel(collectionInfo.GetVchannel(), wal)
	}
	return nil
}

func (f *flusherImpl) RegisterVChannel(vchannel string, wal wal.WAL) {
	if f.scanners.Contain(vchannel) {
		return
	}
	f.tasks.GetOrInsert(vchannel, NewChannelTask(f, vchannel, wal))
	f.notify()
	log.Info("flusher register vchannel done", zap.String("vchannel", vchannel))
}

func (f *flusherImpl) UnregisterPChannel(pchannel string) {
	f.tasks.Range(func(vchannel string, task ChannelTask) bool {
		if funcutil.ToPhysicalChannel(vchannel) == pchannel {
			f.UnregisterVChannel(vchannel)
		}
		return true
	})
	f.scanners.Range(func(vchannel string, scanner wal.Scanner) bool {
		if funcutil.ToPhysicalChannel(vchannel) == pchannel {
			f.UnregisterVChannel(vchannel)
		}
		return true
	})
}

func (f *flusherImpl) UnregisterVChannel(vchannel string) {
	if task, ok := f.tasks.Get(vchannel); ok {
		task.Cancel()
	}
	if scanner, ok := f.scanners.GetAndRemove(vchannel); ok {
		err := scanner.Close()
		if err != nil {
			log.Warn("scanner error", zap.String("vchannel", vchannel), zap.Error(err))
		}
	}
	f.fgMgr.RemoveFlowgraph(vchannel)
	f.wbMgr.RemoveChannel(vchannel)
	log.Info("flusher unregister vchannel done", zap.String("vchannel", vchannel))
}

func (f *flusherImpl) notify() {
	select {
	case f.notifyCh <- struct{}{}:
	default:
	}
}

func (f *flusherImpl) Start() {
	f.stopWg.Add(1)
	f.wbMgr.Start()
	go f.cpUpdater.Start()
	go func() {
		defer f.stopWg.Done()
		for {
			select {
			case <-f.stopChan.CloseCh():
				log.Info("flusher exited")
				return
			case <-f.notifyCh:
				futures := make([]*conc.Future[any], 0)
				f.tasks.Range(func(vchannel string, task ChannelTask) bool {
					future := GetExecPool().Submit(func() (any, error) {
						err := task.Run()
						if err != nil {
							log.Warn("build pipeline failed", zap.String("vchannel", vchannel), zap.Error(err))
							// Notify to trigger retry.
							f.notify()
							return nil, err
						}
						f.tasks.Remove(vchannel)
						return nil, nil
					})
					futures = append(futures, future)
					return true
				})
				_ = conc.AwaitAll(futures...)
			}
		}
	}()
}

func (f *flusherImpl) Stop() {
	f.stopChan.Close()
	f.stopWg.Wait()
	f.scanners.Range(func(vchannel string, scanner wal.Scanner) bool {
		err := scanner.Close()
		if err != nil {
			log.Warn("scanner error", zap.String("vchannel", vchannel), zap.Error(err))
		}
		return true
	})
	f.fgMgr.ClearFlowgraphs()
	f.wbMgr.Stop()
	f.cpUpdater.Close()
}
