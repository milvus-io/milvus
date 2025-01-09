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
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	adaptor2 "github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type LifetimeState int

const (
	Pending LifetimeState = iota
	Cancel
	Fail
	Done
)

var errChannelLifetimeUnrecoverable = errors.New("channel lifetime unrecoverable")

type ChannelLifetime interface {
	Run() error
	Cancel()
}

type channelLifetime struct {
	mu       sync.Mutex
	state    LifetimeState
	vchannel string
	wal      wal.WAL
	scanner  wal.Scanner
	f        *flusherImpl
}

func NewChannelLifetime(f *flusherImpl, vchannel string, wal wal.WAL) ChannelLifetime {
	return &channelLifetime{
		state:    Pending,
		f:        f,
		vchannel: vchannel,
		wal:      wal,
	}
}

func (c *channelLifetime) Run() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Cancel || c.state == Done {
		return nil
	}
	if c.state == Fail {
		return errChannelLifetimeUnrecoverable
	}
	log.Info("start to build pipeline", zap.String("vchannel", c.vchannel))

	// Get recovery info from datacoord.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	pipelineParams, err := c.f.getPipelineParams(ctx)
	if err != nil {
		return err
	}

	dc, err := resource.Resource().DataCoordClient().GetWithContext(ctx)
	if err != nil {
		return errors.Wrap(err, "At Get DataCoordClient")
	}
	resp, err := dc.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: c.vchannel})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	// The channel has been dropped, skip to recover it.
	if len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64 {
		log.Info("channel has been dropped, skip to create flusher for vchannel", zap.String("vchannel", c.vchannel))
		c.state = Fail
		return errChannelLifetimeUnrecoverable
	}

	// Convert common.MessageID to message.messageID.
	messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytes(c.wal.WALName(), resp.GetInfo().GetSeekPosition().GetMsgID())

	// Create scanner.
	policy := options.DeliverPolicyStartFrom(messageID)
	handler := adaptor2.NewMsgPackAdaptorHandler()
	ro := wal.ReadOption{
		VChannel:       c.vchannel,
		DeliverPolicy:  policy,
		MesasgeHandler: handler,
	}
	scanner, err := c.wal.Read(ctx, ro)
	if err != nil {
		return err
	}

	// Build and add pipeline.
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx, pipelineParams,
		// TODO fubang add the db properties
		&datapb.ChannelWatchInfo{Vchan: resp.GetInfo(), Schema: resp.GetSchema()}, handler.Chan(), func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _, _ := tt.Binlogs()
				resource.Resource().SegmentAssignStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
					BinLogCounterIncr:     1,
					BinLogFileCounterIncr: uint64(len(insertLogs)),
				})
			}
		},
		func() { go func() { c.Cancel() }() },
	)
	if err != nil {
		handler.Close()
		return err
	}
	ds.Start()
	c.f.fgMgr.AddFlowgraph(ds)
	c.scanner = scanner
	c.state = Done

	log.Info("build pipeline done", zap.String("vchannel", c.vchannel))
	return nil
}

func (c *channelLifetime) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case Pending:
		c.state = Cancel
	case Cancel, Fail:
		return
	case Done:
		err := c.scanner.Close()
		if err != nil {
			log.Warn("scanner error", zap.String("vchannel", c.vchannel), zap.Error(err))
		}
		c.f.fgMgr.RemoveFlowgraph(c.vchannel)
		c.f.wbMgr.RemoveChannel(c.vchannel)
		log.Info("flusher unregister vchannel done", zap.String("vchannel", c.vchannel))
	}
}
