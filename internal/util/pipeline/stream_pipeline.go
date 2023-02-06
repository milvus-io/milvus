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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
)

type StreamPipeline interface {
	Pipeline
	ConsumeMsgStream(position *internalpb.MsgPosition) error
}

type streamPipeline struct {
	*pipeline
	instream  msgstream.MsgStream
	startOnce sync.Once
	vChannel  string

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
		case msg := <-p.instream.Chan():
			log.RatedDebug(10, "stream pipeline fetch msg", zap.Int("sum", len(msg.Msgs)))
			p.nodes[0].inputChannel <- msg
		}
	}
}

func (p *streamPipeline) ConsumeMsgStream(position *internalpb.MsgPosition) error {
	p.instream.AsConsumer([]string{position.ChannelName}, position.MsgGroup, mqwrapper.SubscriptionPositionUnknown)
	start := time.Now()
	err := p.instream.Seek([]*internalpb.MsgPosition{position})
	ts, _ := tsoutil.ParseTS(position.GetTimestamp())
	log.Info("stream pipeline seeks from position",
		zap.String("pchannel", position.ChannelName),
		zap.String("vchannel", p.vChannel),
		zap.Time("checkpointTs", ts),
		zap.Duration("tsLag", time.Since(ts)),
		zap.Duration("elapse", time.Since(start)),
	)
	return err
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
		p.instream.Close()
		p.pipeline.Close()
	})
}

func NewPipelineWithStream(instream msgstream.MsgStream, nodeTtInterval time.Duration, enableTtChecker bool, vChannel string) StreamPipeline {
	pipeline := &streamPipeline{
		pipeline: &pipeline{
			nodes:           []*nodeCtx{},
			nodeTtInterval:  nodeTtInterval,
			enableTtChecker: enableTtChecker,
		},
		instream: instream,
		vChannel: vChannel,
		closeCh:  make(chan struct{}),
		closeWg:  sync.WaitGroup{},
	}

	return pipeline
}
