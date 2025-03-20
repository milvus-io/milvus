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

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type StreamPipeline interface {
	Pipeline
	ConsumeMsgStream(ctx context.Context, position *msgpb.MsgPosition) error
	Status() string
}

type streamPipeline struct {
	pipeline        *pipeline
	input           <-chan *msgstream.MsgPack
	scanner         streaming.Scanner
	dispatcher      msgdispatcher.Client
	startOnce       sync.Once
	vChannel        string
	replicateConfig *msgstream.ReplicateConfig

	closeCh   chan struct{} // notify work to exit
	closeWg   sync.WaitGroup
	closeOnce sync.Once

	lastAccessTime *atomic.Time
}

func (p *streamPipeline) work() {
	defer p.closeWg.Done()
	for {
		select {
		case <-p.closeCh:
			log.Ctx(context.TODO()).Debug("stream pipeline input closed")
			return
		case msg := <-p.input:
			p.lastAccessTime.Store(time.Now())
			log.Ctx(context.TODO()).RatedDebug(10, "stream pipeline fetch msg", zap.Int("sum", len(msg.Msgs)))
			p.pipeline.inputChannel <- msg
			p.pipeline.process()
		}
	}
}

// Status returns the status of the pipeline, it will return "Healthy" if the input node
// has received any msg in the last nodeTtInterval
func (p *streamPipeline) Status() string {
	diff := time.Since(p.lastAccessTime.Load())
	if diff > p.pipeline.nodeTtInterval {
		return fmt.Sprintf("input node hasn't received any msg in the last %s", diff.String())
	}
	return "Healthy"
}

func (p *streamPipeline) ConsumeMsgStream(ctx context.Context, position *msgpb.MsgPosition) error {
	log := log.Ctx(ctx)
	var err error
	if position == nil {
		log.Error("seek stream to nil position")
		return ErrNilPosition
	}

	if streamingutil.IsStreamingServiceEnabled() {
		startFrom := adaptor.MustGetMessageIDFromMQWrapperIDBytes(streaming.WAL().WALName(), position.GetMsgID())
		log.Info(
			"stream pipeline seeks from position with scanner",
			zap.String("channel", position.GetChannelName()),
			zap.Any("startFromMessageID", startFrom),
			zap.Uint64("timestamp", position.GetTimestamp()),
		)
		handler := adaptor.NewMsgPackAdaptorHandler()
		p.scanner = streaming.WAL().Read(ctx, streaming.ReadOption{
			VChannel:      position.GetChannelName(),
			DeliverPolicy: options.DeliverPolicyStartFrom(startFrom),
			DeliverFilters: []options.DeliverFilter{
				// only consume messages with timestamp >= position timestamp
				options.DeliverFilterTimeTickGTE(position.GetTimestamp()),
				// only consume insert and delete messages
				options.DeliverFilterMessageType(message.MessageTypeInsert, message.MessageTypeDelete),
			},
			MessageHandler: handler,
		})
		p.input = handler.Chan()
		return nil
	}

	start := time.Now()
	p.input, err = p.dispatcher.Register(ctx, &msgdispatcher.StreamConfig{
		VChannel:        p.vChannel,
		Pos:             position,
		SubPos:          common.SubscriptionPositionUnknown,
		ReplicateConfig: p.replicateConfig,
	})
	if err != nil {
		log.Error("dispatcher register failed after retried", zap.String("channel", position.ChannelName), zap.Error(err))
		p.dispatcher.Deregister(p.vChannel)
		return WrapErrRegDispather(err)
	}

	ts, _ := tsoutil.ParseTS(position.GetTimestamp())
	log.Info("stream pipeline seeks from position with msgDispatcher",
		zap.String("pchannel", position.ChannelName),
		zap.String("vchannel", p.vChannel),
		zap.Time("checkpointTs", ts),
		zap.Duration("tsLag", time.Since(ts)),
		zap.Duration("elapse", time.Since(start)),
	)
	return nil
}

func (p *streamPipeline) Add(nodes ...Node) {
	p.pipeline.Add(nodes...)
}

func (p *streamPipeline) Start() error {
	var err error
	p.startOnce.Do(func() {
		err = p.pipeline.Start()
		p.closeWg.Add(1)
		go p.work()
	})
	return err
}

func (p *streamPipeline) Close() {
	p.closeOnce.Do(func() {
		// close datasource first
		p.dispatcher.Deregister(p.vChannel)
		// close stream input
		close(p.closeCh)
		p.closeWg.Wait()
		if p.scanner != nil {
			p.scanner.Close()
		}
		// close the underline pipeline
		p.pipeline.Close()
	})
}

func NewPipelineWithStream(dispatcher msgdispatcher.Client,
	nodeTtInterval time.Duration,
	enableTtChecker bool,
	vChannel string,
	replicateConfig *msgstream.ReplicateConfig,
) StreamPipeline {
	pipeline := &streamPipeline{
		pipeline: &pipeline{
			nodes:           []*nodeCtx{},
			nodeTtInterval:  nodeTtInterval,
			enableTtChecker: enableTtChecker,
		},
		dispatcher:      dispatcher,
		vChannel:        vChannel,
		replicateConfig: replicateConfig,
		closeCh:         make(chan struct{}),
		closeWg:         sync.WaitGroup{},
		lastAccessTime:  atomic.NewTime(time.Now()),
	}

	return pipeline
}
