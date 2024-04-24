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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type Pipeline interface {
	Add(node ...Node)
	Start() error
	Close()
}
type pipeline struct {
	nodes []*nodeCtx

	inputChannel    chan Msg
	nodeTtInterval  time.Duration
	enableTtChecker bool

	closeCh   chan struct{} // notify work to exit
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

func (p *pipeline) Add(nodes ...Node) {
	for _, node := range nodes {
		p.addNode(node)
	}
}

func (p *pipeline) addNode(node Node) {
	nodeCtx := newNodeCtx(node)
	if p.enableTtChecker {
		nodeCtx.checker = timerecord.GetGroupChecker("fgNode", p.nodeTtInterval, func(list []string) {
			log.Warn("some node(s) haven't received input", zap.Strings("list", list), zap.Duration("duration ", p.nodeTtInterval))
		})
	}

	if len(p.nodes) != 0 {
		p.nodes[len(p.nodes)-1].next = nodeCtx
	} else {
		p.inputChannel = nodeCtx.inputChannel
	}

	p.nodes = append(p.nodes, nodeCtx)
}

func (p *pipeline) Start() error {
	if len(p.nodes) == 0 {
		return ErrEmptyPipeline
	}

	p.closeWg.Add(1)
	go p.work()
	return nil
}

func (p *pipeline) Close() {
	p.closeOnce.Do(func() {
		close(p.closeCh)
		p.closeWg.Wait()
	})
}

func (p *pipeline) work() {
	defer p.closeWg.Done()

	for _, nodeCtx := range p.nodes {
		name := fmt.Sprintf("nodeCtxTtChecker-%s", nodeCtx.node.Name())
		if nodeCtx.checker != nil {
			nodeCtx.checker.Check(name)
			defer nodeCtx.checker.Remove(name)
		}
	}

	for {
		select {
		// close
		case <-p.closeCh:
			log.Debug("node ctx manager closed")
			return

		default:
			curNode := p.nodes[0]
			for curNode != nil {
				if len(curNode.inputChannel) == 0 {
					break
				}

				input := <-curNode.inputChannel
				output := curNode.node.Operate(input)
				if curNode.next != nil && output != nil {
					curNode.next.inputChannel <- output
				}
				curNode = curNode.next
			}
		}
	}
}
