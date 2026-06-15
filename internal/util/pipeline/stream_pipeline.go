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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type StreamPipeline interface {
	Pipeline
	ConsumeMsgStream(ctx context.Context, position *msgpb.MsgPosition) error
	Status() string
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

	lastAccessTime          *atomic.Time
	emptyTimeTickSlowdowner *emptyTimeTickSlowdowner
	msgPackBatcher          MsgPackBatcher
}

func (p *streamPipeline) work() {
	defer p.closeWg.Done()
	var pending *msgstream.MsgPack
	for {
		var msg *msgstream.MsgPack
		if pending != nil {
			msg = pending
			pending = nil
		} else {
			select {
			case <-p.closeCh:
				log.Ctx(context.TODO()).Debug("stream pipeline input closed")
				return
			case msgPack, ok := <-p.input:
				if !ok {
					log.Ctx(context.TODO()).Debug("stream pipeline input closed")
					return
				}
				msg = msgPack
			}
		}

		p.lastAccessTime.Store(time.Now())
		// Currently, milvus use the timetick to synchronize the system periodically,
		// so the wal will still produce empty timetick message after the last write operation is done.
		// When there're huge amount of vchannel in one pchannel, it will introduce a great overhead.
		// So we filter out the empty time tick message as much as possible.
		// TODO: After 3.0, we can remove the filter logic by LSN+MVCC.
		if p.emptyTimeTickSlowdowner.Filter(msg) {
			continue
		}

		if p.msgPackBatcher != nil {
			msg, pending = p.msgPackBatcher.Batch(msg, p.input)
		}

		log.Ctx(context.TODO()).RatedDebug(10, "stream pipeline fetch msg", zap.Int("sum", len(msg.Msgs)))
		p.pipeline.inputChannel <- msg
		p.pipeline.process()
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

	start := time.Now()
	p.input, err = p.dispatcher.Register(ctx, &msgdispatcher.StreamConfig{
		VChannel: p.vChannel,
		Pos:      position,
		SubPos:   common.SubscriptionPositionUnknown,
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
		zap.Stringer("walName", position.WALName),
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
	lastestMVCCTimeTickGetter LastestMVCCTimeTickGetter,
	options ...StreamPipelineOption,
) StreamPipeline {
	pipeline := &streamPipeline{
		pipeline: &pipeline{
			nodes:           []*nodeCtx{},
			nodeTtInterval:  nodeTtInterval,
			enableTtChecker: enableTtChecker,
		},
		dispatcher:              dispatcher,
		vChannel:                vChannel,
		closeCh:                 make(chan struct{}),
		closeWg:                 sync.WaitGroup{},
		lastAccessTime:          atomic.NewTime(time.Now()),
		emptyTimeTickSlowdowner: newEmptyTimeTickSlowdowner(lastestMVCCTimeTickGetter, vChannel),
	}

	for _, option := range options {
		option(pipeline)
	}

	return pipeline
}

type StreamPipelineOption func(*streamPipeline)

func WithMsgPackBatcher(batcher MsgPackBatcher) StreamPipelineOption {
	return func(pipeline *streamPipeline) {
		pipeline.msgPackBatcher = batcher
	}
}

type MsgPackBatcher interface {
	Batch(first *msgstream.MsgPack, input <-chan *msgstream.MsgPack) (merged *msgstream.MsgPack, pending *msgstream.MsgPack)
}

type dmlMsgPackBatcher struct {
	maxPacks int
}

func NewDMLMsgPackBatcher(maxPacks int) MsgPackBatcher {
	return &dmlMsgPackBatcher{maxPacks: maxPacks}
}

func (b *dmlMsgPackBatcher) Batch(first *msgstream.MsgPack, input <-chan *msgstream.MsgPack) (*msgstream.MsgPack, *msgstream.MsgPack) {
	if b.maxPacks <= 1 || !isDMLMsgPack(first) {
		return first, nil
	}

	packs := []*msgstream.MsgPack{first}
	for len(packs) < b.maxPacks {
		select {
		case next, ok := <-input:
			if !ok {
				return mergeMsgPacks(packs), nil
			}
			if !isDMLMsgPack(next) {
				return mergeMsgPacks(packs), next
			}
			packs = append(packs, next)
		default:
			return mergeMsgPacks(packs), nil
		}
	}
	return mergeMsgPacks(packs), nil
}

func isDMLMsgPack(pack *msgstream.MsgPack) bool {
	if pack == nil || len(pack.Msgs) == 0 {
		return false
	}

	for _, msg := range pack.Msgs {
		switch msg.Type() {
		case commonpb.MsgType_Insert, commonpb.MsgType_Delete:
		default:
			return false
		}
	}
	return true
}

func mergeMsgPacks(packs []*msgstream.MsgPack) *msgstream.MsgPack {
	if len(packs) == 1 {
		return packs[0]
	}

	first := packs[0]
	last := packs[len(packs)-1]
	merged := &msgstream.MsgPack{
		BeginTs:        first.BeginTs,
		EndTs:          last.EndTs,
		StartPositions: first.StartPositions,
		EndPositions:   last.EndPositions,
		Msgs:           make([]msgstream.TsMsg, 0),
	}
	for _, pack := range packs {
		merged.Msgs = append(merged.Msgs, pack.Msgs...)
	}
	return merged
}
