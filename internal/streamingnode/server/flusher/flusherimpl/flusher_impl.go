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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
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
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ flusher.Flusher = (*flusherImpl)(nil)

type flusherImpl struct {
	fgMgr        pipeline.FlowgraphManager
	wbMgr        writebuffer.BufferManager
	syncMgr      syncmgr.SyncManager
	cpUpdater    *syncutil.Future[*util.ChannelCheckpointUpdater]
	chunkManager storage.ChunkManager

	channelLifetimes *typeutil.ConcurrentMap[string, ChannelLifetime]

	notifyCh chan struct{}
	notifier *syncutil.AsyncTaskNotifier[struct{}]
}

func NewFlusher(chunkManager storage.ChunkManager) flusher.Flusher {
	syncMgr := syncmgr.NewSyncManager(chunkManager)
	wbMgr := writebuffer.NewManager(syncMgr)
	return &flusherImpl{
		fgMgr:            pipeline.NewFlowgraphManager(),
		wbMgr:            wbMgr,
		syncMgr:          syncMgr,
		cpUpdater:        syncutil.NewFuture[*util.ChannelCheckpointUpdater](),
		chunkManager:     chunkManager,
		channelLifetimes: typeutil.NewConcurrentMap[string, ChannelLifetime](),
		notifyCh:         make(chan struct{}, 1),
		notifier:         syncutil.NewAsyncTaskNotifier[struct{}](),
	}
}

func (f *flusherImpl) RegisterPChannel(pchannel string, wal wal.WAL) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := resource.Resource().RootCoordClient().GetWithContext(ctx)
	if err != nil {
		return errors.Wrap(err, "At Get RootCoordClient")
	}
	resp, err := rc.GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
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
	_, ok := f.channelLifetimes.GetOrInsert(vchannel, NewChannelLifetime(f, vchannel, wal))
	if !ok {
		log.Info("flusher register vchannel done", zap.String("vchannel", vchannel))
	}
	f.notify()
}

func (f *flusherImpl) UnregisterPChannel(pchannel string) {
	f.channelLifetimes.Range(func(vchannel string, _ ChannelLifetime) bool {
		if funcutil.ToPhysicalChannel(vchannel) == pchannel {
			f.UnregisterVChannel(vchannel)
		}
		return true
	})
}

func (f *flusherImpl) UnregisterVChannel(vchannel string) {
	if clt, ok := f.channelLifetimes.GetAndRemove(vchannel); ok {
		clt.Cancel()
	}
}

func (f *flusherImpl) notify() {
	select {
	case f.notifyCh <- struct{}{}:
	default:
	}
}

func (f *flusherImpl) Start() {
	f.wbMgr.Start()
	go func() {
		defer f.notifier.Finish(struct{}{})
		dc, err := resource.Resource().DataCoordClient().GetWithContext(f.notifier.Context())
		if err != nil {
			return
		}
		broker := broker.NewCoordBroker(dc, paramtable.GetNodeID())
		cpUpdater := util.NewChannelCheckpointUpdater(broker)
		go cpUpdater.Start()
		f.cpUpdater.Set(cpUpdater)

		backoff := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
			Default: 5 * time.Second,
			Backoff: typeutil.BackoffConfig{
				InitialInterval: 50 * time.Millisecond,
				Multiplier:      2.0,
				MaxInterval:     5 * time.Second,
			},
		})

		var nextTimer <-chan time.Time
		for {
			select {
			case <-f.notifier.Context().Done():
				log.Info("flusher exited")
				return
			case <-f.notifyCh:
				nextTimer = f.handle(backoff)
			case <-nextTimer:
				nextTimer = f.handle(backoff)
			}
		}
	}()
}

func (f *flusherImpl) handle(backoff *typeutil.BackoffTimer) <-chan time.Time {
	futures := make([]*conc.Future[any], 0)
	failureCnt := atomic.NewInt64(0)
	f.channelLifetimes.Range(func(vchannel string, lifetime ChannelLifetime) bool {
		future := GetExecPool().Submit(func() (any, error) {
			err := lifetime.Run()
			if errors.Is(err, errChannelLifetimeUnrecoverable) {
				log.Warn("channel lifetime is unrecoverable, removed", zap.String("vchannel", vchannel))
				f.channelLifetimes.Remove(vchannel)
				return nil, nil
			}
			if err != nil {
				log.Warn("build pipeline failed", zap.String("vchannel", vchannel), zap.Error(err))
				failureCnt.Inc()
				return nil, err
			}
			return nil, nil
		})
		futures = append(futures, future)
		return true
	})
	_ = conc.BlockOnAll(futures...)

	if failureCnt.Load() > 0 {
		backoff.EnableBackoff()
		nextTimer, interval := backoff.NextTimer()
		log.Warn("flusher lifetime trasition failed, retry with backoff...", zap.Int64("failureCnt", failureCnt.Load()), zap.Duration("interval", interval))
		return nextTimer
	}
	// There's a failure, do no backoff.
	backoff.DisableBackoff()
	return nil
}

func (f *flusherImpl) Stop() {
	f.notifier.Cancel()
	f.notifier.BlockUntilFinish()
	f.channelLifetimes.Range(func(vchannel string, lifetime ChannelLifetime) bool {
		lifetime.Cancel()
		return true
	})
	f.fgMgr.ClearFlowgraphs()
	f.wbMgr.Stop()
	if f.cpUpdater.Ready() {
		f.cpUpdater.Get().Close()
	}
}

func (f *flusherImpl) getPipelineParams(ctx context.Context) (*util.PipelineParams, error) {
	dc, err := resource.Resource().DataCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	cpUpdater, err := f.cpUpdater.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &util.PipelineParams{
		Ctx:                context.Background(),
		Broker:             broker.NewCoordBroker(dc, paramtable.GetNodeID()),
		SyncMgr:            f.syncMgr,
		ChunkManager:       f.chunkManager,
		WriteBufferManager: f.wbMgr,
		CheckpointUpdater:  cpUpdater,
		Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
		MsgHandler:         newMsgHandler(f.wbMgr),
	}, nil
}
