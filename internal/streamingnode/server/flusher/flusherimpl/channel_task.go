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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	adaptor2 "github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type TaskState int

const (
	Pending TaskState = iota
	Cancel
	Done
)

type ChannelTask interface {
	Run() error
	Cancel()
}

type channelTask struct {
	mu       sync.Mutex
	state    TaskState
	f        *flusherImpl
	vchannel string
	wal      wal.WAL
}

func NewChannelTask(f *flusherImpl, vchannel string, wal wal.WAL) ChannelTask {
	return &channelTask{
		state:    Pending,
		f:        f,
		vchannel: vchannel,
		wal:      wal,
	}
}

func (c *channelTask) Run() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Cancel {
		return nil
	}
	if c.f.fgMgr.HasFlowgraph(c.vchannel) {
		return nil
	}
	log.Info("start to build pipeline", zap.String("vchannel", c.vchannel))

	// Get recovery info from datacoord.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	resp, err := resource.Resource().DataCoordClient().
		GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: c.vchannel})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}

	// Convert common.MessageID to message.messageID.
	messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytes(c.wal.WALName(), resp.GetInfo().GetSeekPosition().GetMsgID())

	// Create scanner.
	policy := options.DeliverPolicyStartFrom(messageID)
	handler := adaptor2.NewMsgPackAdaptorHandler()
	ro := wal.ReadOption{
		DeliverPolicy: policy,
		MessageFilter: []options.DeliverFilter{
			options.DeliverFilterVChannel(c.vchannel),
		},
		MesasgeHandler: handler,
	}
	scanner, err := c.wal.Read(ctx, ro)
	if err != nil {
		return err
	}

	// Build and add pipeline.
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx, c.f.pipelineParams,
		&datapb.ChannelWatchInfo{Vchan: resp.GetInfo(), Schema: resp.GetSchema()}, handler.Chan())
	if err != nil {
		return err
	}
	ds.Start()
	c.f.fgMgr.AddFlowgraph(ds)
	c.f.scanners.Insert(c.vchannel, scanner)
	c.state = Done

	log.Info("build pipeline done", zap.String("vchannel", c.vchannel))
	return nil
}

func (c *channelTask) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case Pending:
		c.state = Cancel
	case Cancel:
		return
	case Done:
		if scanner, ok := c.f.scanners.GetAndRemove(c.vchannel); ok {
			err := scanner.Close()
			if err != nil {
				log.Warn("scanner error", zap.String("vchannel", c.vchannel), zap.Error(err))
			}
		}
		c.f.fgMgr.RemoveFlowgraph(c.vchannel)
		c.f.wbMgr.RemoveChannel(c.vchannel)
		log.Info("flusher unregister vchannel done", zap.String("vchannel", c.vchannel))
	}
}
