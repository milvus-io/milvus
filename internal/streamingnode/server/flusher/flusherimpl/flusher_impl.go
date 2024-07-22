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

	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	util2 "github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ flusher.Flusher = (*flusherImpl)(nil)

type flusherImpl struct {
	tasks *typeutil.ConcurrentMap[string, wal.WAL] // unwatched vchannels

	fgMgr     pipeline.FlowgraphManager
	syncMgr   syncmgr.SyncManager
	wbMgr     writebuffer.BufferManager
	cpUpdater *util2.ChannelCheckpointUpdater

	stopOnce sync.Once
	stopChan chan struct{}
}

func NewFlusher(params *util2.PipelineParams) flusher.Flusher {
	fgMgr := pipeline.NewFlowgraphManager()
	return &flusherImpl{
		tasks:     typeutil.NewConcurrentMap[string, wal.WAL](),
		fgMgr:     fgMgr,
		syncMgr:   params.SyncMgr,
		wbMgr:     params.WriteBufferManager,
		cpUpdater: params.CheckpointUpdater,
		stopOnce:  sync.Once{},
		stopChan:  make(chan struct{}),
	}
}

func (f *flusherImpl) Open(wal wal.WAL) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := resource.Resource().RootCoordClient().GetVChannels(ctx, &rootcoordpb.GetVChannelsRequest{
		Pchannel: wal.WALName(),
	})
	if err != nil {
		return err
	}
	for _, vchannel := range resp.GetVchannels() {
		f.tasks.Insert(vchannel, wal)
	}
	return nil
}

func (f *flusherImpl) Close(pchannel string) {
	f.fgMgr.RemoveFlowgraphsByPChannel(pchannel)
}

func (f *flusherImpl) Register(vchannel string, wal wal.WAL) {
	f.tasks.Insert(vchannel, wal)
}

func (f *flusherImpl) Deregister(vchannel string) {
	f.fgMgr.RemoveFlowgraph(vchannel)
}

func (f *flusherImpl) Start() {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-f.stopChan:
				log.Info("flusher stopped")
				return
			case <-ticker.C:
				f.tasks.Range(func(vchannel string, wal wal.WAL) bool {
					log := log.With(zap.String("vchannel", vchannel))
					err := f.buildPipeline(vchannel, wal)
					if err != nil {
						log.Warn("build pipeline failed", zap.Error(err))
						return true
					}
					log.Info("build pipeline done")
					return true
				})
			}
		}
	}()
}

func (f *flusherImpl) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopChan)
		f.fgMgr.ClearFlowgraphs()
		f.wbMgr.Stop()
		f.cpUpdater.Close()
	})
}

func (f *flusherImpl) buildPipeline(vchannel string, w wal.WAL) error {
	if f.fgMgr.HasFlowgraph(vchannel) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	resp, err := resource.Resource().DataCoordClient().
		GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
	if err != nil {
		return err
	}

	var messageID message.MessageID // TODO: parse info.GetSeekPosition().GetMsgID() into message.MessageID
	policy := options.DeliverPolicyStartFrom(messageID)
	filter := func(msg message.ImmutableMessage) bool {
		return msg.VChannel() == vchannel
	}
	ro := wal.ReadOption{DeliverPolicy: policy, MessageFilter: filter}
	scanner, err := w.Read(ctx, ro)
	if err != nil {
		return err
	}

	scanner.Chan() // TODO: resolve it, pass scanner into it
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx, GetPipelineParams(),
		&datapb.ChannelWatchInfo{Vchan: resp.GetInfo(), Schema: resp.GetSchema()}, nil)
	if err != nil {
		return err
	}
	ds.Start()
	f.fgMgr.AddFlowgraph(ds)
	return nil
}
