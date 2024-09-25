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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type StreamPipeline interface {
	Pipeline
	ConsumeMsgStream(position *msgpb.MsgPosition) error
}

type streamPipeline struct {
	pipeline   *pipeline
	input      <-chan *msgstream.MsgPack
	scanner    streaming.Scanner
	dispatcher msgdispatcher.Client
	startOnce  sync.Once
	vChannel   string

	closeCh   chan struct{} // notify work to exit
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

func (p *streamPipeline) work() {
	defer p.closeWg.Done()
	for {
		select {
		case <-p.closeCh:
			log.Debug("stream pipeline input closed")
			return
		case msg := <-p.input:
			log.RatedDebug(10, "stream pipeline fetch msg", zap.Int("sum", len(msg.Msgs)))
			p.pipeline.inputChannel <- msg
			p.pipeline.process()
		}
	}
}

func (p *streamPipeline) ConsumeMsgStream(position *msgpb.MsgPosition) error {
	var err error
	if position == nil {
		log.Error("seek stream to nil position")
		return ErrNilPosition
	}

	if streamingutil.IsStreamingServiceEnabled() {
		startFrom := adaptor.MustGetMessageIDFromMQWrapperIDBytes("pulsar", position.GetMsgID())
		log.Info(
			"stream pipeline seeks from position with scanner",
			zap.String("channel", position.GetChannelName()),
			zap.Any("startFromMessageID", startFrom),
			zap.Uint64("timestamp", position.GetTimestamp()),
		)
		handler := adaptor.NewMsgPackAdaptorHandler()
		p.scanner = streaming.WAL().Read(context.Background(), streaming.ReadOption{
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
	p.input, err = p.dispatcher.Register(context.TODO(), p.vChannel, position, common.SubscriptionPositionUnknown)
	if err != nil {
		log.Error("dispatcher register failed", zap.String("channel", position.ChannelName))
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
		close(p.closeCh)
		p.closeWg.Wait()
		if p.scanner != nil {
			p.scanner.Close()
		}
		p.dispatcher.Deregister(p.vChannel)
		p.pipeline.Close()
	})
}

func NewPipelineWithStream(dispatcher msgdispatcher.Client, nodeTtInterval time.Duration, enableTtChecker bool, vChannel string) StreamPipeline {
	pipeline := &streamPipeline{
		pipeline: &pipeline{
			nodes:           []*nodeCtx{},
			nodeTtInterval:  nodeTtInterval,
			enableTtChecker: enableTtChecker,
		},
		dispatcher: dispatcher,
		vChannel:   vChannel,
		closeCh:    make(chan struct{}),
		closeWg:    sync.WaitGroup{},
	}

	return pipeline
}
