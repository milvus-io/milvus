// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package flowgraph

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type Node interface {
	Name() string
	MaxQueueLength() int32
	MaxParallelism() int32
	Operate(in []Msg) []Msg
	IsInputNode() bool
	Close()
}

type BaseNode struct {
	maxQueueLength int32
	maxParallelism int32
}

type nodeCtx struct {
	node                   Node
	inputChannels          []chan Msg
	inputMessages          []Msg
	downstream             []*nodeCtx
	downstreamInputChanIdx map[string]int

	NumActiveTasks    int64
	NumCompletedTasks int64
}

func (nodeCtx *nodeCtx) Start(ctx context.Context, wg *sync.WaitGroup) {
	if nodeCtx.node.IsInputNode() {
		inStream, ok := nodeCtx.node.(*InputNode)
		if !ok {
			log.Error("Invalid inputNode")
		}
		(*inStream.inStream).Start()
	}

	for {
		select {
		case <-ctx.Done():
			if nodeCtx.node.IsInputNode() {
				inStream, ok := nodeCtx.node.(*InputNode)
				if !ok {
					log.Error("Invalid inputNode")
				}
				(*inStream.inStream).Close()
				log.Debug("message stream closed",
					zap.Any("node name", inStream.name),
				)
			}
			wg.Done()
			return
		default:
			// inputs from inputsMessages for Operate
			inputs := make([]Msg, 0)

			var res []Msg
			if !nodeCtx.node.IsInputNode() {
				nodeCtx.collectInputMessages(ctx)
				inputs = nodeCtx.inputMessages
			}
			n := nodeCtx.node
			res = n.Operate(inputs)

			downstreamLength := len(nodeCtx.downstreamInputChanIdx)
			if len(nodeCtx.downstream) < downstreamLength {
				log.Warn("", zap.Any("nodeCtx.downstream length", len(nodeCtx.downstream)))
			}
			if len(res) < downstreamLength {
				// log.Println("node result length = ", len(res))
				break
			}

			w := sync.WaitGroup{}
			for i := 0; i < downstreamLength; i++ {
				w.Add(1)
				go nodeCtx.downstream[i].ReceiveMsg(&w, res[i], nodeCtx.downstreamInputChanIdx[nodeCtx.downstream[i].node.Name()])
			}
			w.Wait()
		}
	}
}

func (nodeCtx *nodeCtx) Close() {
	// data race with nodeCtx.ReceiveMsg { nodeCtx.inputChannels[inputChanIdx] <- msg }
	//for _, channel := range nodeCtx.inputChannels {
	//	close(channel)
	//	log.Warn("close inputChannel")
	//}
	nodeCtx.node.Close()
}

func (nodeCtx *nodeCtx) ReceiveMsg(wg *sync.WaitGroup, msg Msg, inputChanIdx int) {
	defer wg.Done()
	defer func() {
		err := recover()
		if err != nil {
			log.Warn(fmt.Sprintln(err))
		}
	}()
	nodeCtx.inputChannels[inputChanIdx] <- msg
	//fmt.Println((*nodeCtx.node).Name(), "receive to input channel ", inputChanIdx)
}

func (nodeCtx *nodeCtx) collectInputMessages(exitCtx context.Context) {
	inputsNum := len(nodeCtx.inputChannels)
	nodeCtx.inputMessages = make([]Msg, inputsNum)

	// init inputMessages,
	// receive messages from inputChannels,
	// and move them to inputMessages.
	for i := 0; i < inputsNum; i++ {
		channel := nodeCtx.inputChannels[i]
		select {
		case <-exitCtx.Done():
			return
		case msg, ok := <-channel:
			if !ok {
				// TODO: add status
				log.Warn("input channel closed")
				return
			}
			nodeCtx.inputMessages[i] = msg
		}
	}

	// timeTick alignment check
	if len(nodeCtx.inputMessages) > 1 {
		t := nodeCtx.inputMessages[0].TimeTick()
		latestTime := t
		for i := 1; i < len(nodeCtx.inputMessages); i++ {
			if t < nodeCtx.inputMessages[i].TimeTick() {
				latestTime = nodeCtx.inputMessages[i].TimeTick()
			}
		}

		// wait for time tick
		sign := make(chan struct{})
		go func() {
			for i := 0; i < len(nodeCtx.inputMessages); i++ {
				for nodeCtx.inputMessages[i].TimeTick() != latestTime {
					log.Debug("try to align timestamp", zap.Uint64("t1", latestTime), zap.Uint64("t2", nodeCtx.inputMessages[i].TimeTick()))
					channel := nodeCtx.inputChannels[i]
					select {
					case <-exitCtx.Done():
						return
					case msg, ok := <-channel:
						if !ok {
							log.Warn("input channel closed")
							return
						}
						nodeCtx.inputMessages[i] = msg
					}
				}
			}
			sign <- struct{}{}
		}()

		select {
		case <-time.After(10 * time.Second):
			panic("Fatal, misaligned time tick, please restart pulsar")
		case <-sign:
		}

	}
}

func (node *BaseNode) MaxQueueLength() int32 {
	return node.maxQueueLength
}

func (node *BaseNode) MaxParallelism() int32 {
	return node.maxParallelism
}

func (node *BaseNode) SetMaxQueueLength(n int32) {
	node.maxQueueLength = n
}

func (node *BaseNode) SetMaxParallelism(n int32) {
	node.maxParallelism = n
}

func (node *BaseNode) IsInputNode() bool {
	return false
}

func (node *BaseNode) Close() {
	//TODO
}
