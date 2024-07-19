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

package flusher

import (
	"context"
	"github.com/milvus-io/milvus/internal/datanode/pipeline"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Flusher interface {
	// Open ASYNCHRONOUSLY creates and starts pipelines belonging to the pchannel.
	// If a pipeline creation fails, the flusher will keep retrying to create it indefinitely.
	Open(w wal.WAL) error

	// Close SYNCHRONOUSLY stops and removes pipelines belonging to the pchannel.
	Close(pchannel string)

	// Start flusher service.
	Start()

	// Stop flusher, will synchronously flush all remaining data.
	Stop()
}

type flusher struct {
	tasks *typeutil.ConcurrentMap[string, wal.WAL] // unwatched vchannels

	fgMgr     pipeline.FlowgraphManager
	syncMgr   syncmgr.SyncManager
	wbMgr     writebuffer.BufferManager
	cpUpdater *util.ChannelCheckpointUpdater

	stopOnce sync.Once
	stopChan chan struct{}
}

func NewFlusher() Flusher {
	return &flusher{
		tasks:    typeutil.NewConcurrentMap[string, wal.WAL](),
		stopChan: make(chan struct{}),
	}
}

func (f *flusher) Open(wal wal.WAL) error {
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

func (f *flusher) Close(pchannel string) {
	f.fgMgr.RemoveFlowgraphsByPChannel(pchannel)
}

func (f *flusher) Start() {
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

func (f *flusher) buildPipeline(vchannel string, w wal.WAL) error {
	if f.fgMgr.HasFlowgraph(vchannel) {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	resource.Resource().DataCoordClient()

	var info *datapb.VchannelInfo   // TODO: add new GetDataRecoveryInfo rpc
	var messageID message.MessageID // TODO: parse info.GetSeekPosition().GetMsgID() into message.MessageID
	policy := options.DeliverPolicyStartFrom(messageID)
	filterByVchannel := func(msg message.ImmutableMessage) bool {
		return msg.VChannel() == vchannel
	}
	ro := wal.ReadOption{DeliverPolicy: policy, MessageFilter: filterByVchannel}
	scanner, err := w.Read(ctx, ro)
	if err != nil {
		return err
	}

	scanner.Chan()
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx, GetPipelineParams(), &datapb.ChannelWatchInfo{Vchan: info /*TODO: fix schema*/}, nil) // TODO: resolve it, pass scanner into it
	if err != nil {
		return err
	}
	ds.Start()
	f.fgMgr.AddFlowgraph(ds)
	return nil
}

func (f *flusher) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopChan)
		f.fgMgr.ClearFlowgraphs()
		f.wbMgr.Stop()
		f.cpUpdater.Close()
	})
}
