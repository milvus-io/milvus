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
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	adaptor2 "github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var tickDuration = 3 * time.Second

var _ flusher.Flusher = (*flusherImpl)(nil)

type flusherImpl struct {
	fgMgr     pipeline.FlowgraphManager
	syncMgr   syncmgr.SyncManager
	wbMgr     writebuffer.BufferManager
	cpUpdater *util.ChannelCheckpointUpdater

	tasks    *typeutil.ConcurrentMap[string, wal.WAL]     // unwatched vchannels
	scanners *typeutil.ConcurrentMap[string, wal.Scanner] // watched scanners

	stopOnce sync.Once
	stopChan chan struct{}
}

func NewFlusher() flusher.Flusher {
	params := GetPipelineParams()
	fgMgr := pipeline.NewFlowgraphManager()
	return &flusherImpl{
		fgMgr:     fgMgr,
		syncMgr:   params.SyncMgr,
		wbMgr:     params.WriteBufferManager,
		cpUpdater: params.CheckpointUpdater,
		tasks:     typeutil.NewConcurrentMap[string, wal.WAL](),
		scanners:  typeutil.NewConcurrentMap[string, wal.Scanner](),
		stopOnce:  sync.Once{},
		stopChan:  make(chan struct{}),
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
		f.tasks.Insert(collectionInfo.GetVchannel(), wal)
	}
	return nil
}

func (f *flusherImpl) UnregisterPChannel(pchannel string) {
	f.scanners.Range(func(vchannel string, scanner wal.Scanner) bool {
		if funcutil.ToPhysicalChannel(vchannel) != pchannel {
			return true
		}
		f.UnregisterVChannel(vchannel)
		return true
	})
}

func (f *flusherImpl) RegisterVChannel(vchannel string, wal wal.WAL) {
	f.tasks.Insert(vchannel, wal)
}

func (f *flusherImpl) UnregisterVChannel(vchannel string) {
	if scanner, ok := f.scanners.GetAndRemove(vchannel); ok {
		err := scanner.Close()
		if err != nil {
			log.Warn("scanner error", zap.String("vchannel", vchannel), zap.Error(err))
		}
	}
	f.fgMgr.RemoveFlowgraph(vchannel)
	f.wbMgr.RemoveChannel(vchannel)
}

func (f *flusherImpl) Start() {
	f.wbMgr.Start()
	go f.cpUpdater.Start()
	go func() {
		ticker := time.NewTicker(tickDuration)
		defer ticker.Stop()
		for {
			select {
			case <-f.stopChan:
				log.Info("flusher stopped")
				return
			case <-ticker.C:
				f.tasks.Range(func(vchannel string, wal wal.WAL) bool {
					err := f.buildPipeline(vchannel, wal)
					if err != nil {
						log.Warn("build pipeline failed", zap.String("vchannel", vchannel), zap.Error(err))
						return true
					}
					log.Info("build pipeline done", zap.String("vchannel", vchannel))
					f.tasks.Remove(vchannel)
					return true
				})
			}
		}
	}()
}

func (f *flusherImpl) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopChan)
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
	})
}

func (f *flusherImpl) buildPipeline(vchannel string, w wal.WAL) error {
	if f.fgMgr.HasFlowgraph(vchannel) {
		return nil
	}
	log.Info("start to build pipeline", zap.String("vchannel", vchannel))

	// Get recovery info from datacoord.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	resp, err := resource.Resource().DataCoordClient().
		GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}

	// Convert common.MessageID to message.messageID.
	mqWrapperID, err := adaptor.DeserializeToMQWrapperID(resp.GetInfo().GetSeekPosition().GetMsgID(), w.WALName())
	if err != nil {
		return err
	}
	messageID := adaptor.MustGetMessageIDFromMQWrapperID(mqWrapperID)

	// Create scanner.
	policy := options.DeliverPolicyStartFrom(messageID)
	filter := func(msg message.ImmutableMessage) bool { return msg.VChannel() == vchannel }
	handler := adaptor2.NewMsgPackAdaptorHandler()
	ro := wal.ReadOption{
		DeliverPolicy:  policy,
		MessageFilter:  filter,
		MesasgeHandler: handler,
	}
	scanner, err := w.Read(ctx, ro)
	if err != nil {
		return err
	}

	// Build and add pipeline.
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx, GetPipelineParams(),
		&datapb.ChannelWatchInfo{Vchan: resp.GetInfo(), Schema: resp.GetSchema()}, handler.Chan())
	if err != nil {
		return err
	}
	ds.Start()
	f.fgMgr.AddFlowgraph(ds)
	f.scanners.Insert(vchannel, scanner)
	return nil
}
