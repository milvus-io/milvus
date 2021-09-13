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
	"log"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Flow graph basic example: count `d = pow(a) + sqrt(a)`
// nodeA: receive input value a from input channel
// nodeB: count b = pow(a, 2)
// nodeC: count c = sqrt(a)
// nodeD: count d = b + c

type nodeA struct {
	BaseNode
	inputChan chan float64
	a         float64
}

type nodeB struct {
	BaseNode
	b float64
}

type nodeC struct {
	BaseNode
	c float64
}

type nodeD struct {
	BaseNode
	d          float64
	outputChan chan float64
}

type numMsg struct {
	num float64
}

func (m *numMsg) TimeTick() Timestamp {
	return Timestamp(0)
}

func (n *nodeA) Name() string {
	return "NodeA"
}

func (n *nodeA) Operate(in []Msg) []Msg {
	// ignore `in` because nodeA doesn't have any upstream node.
	a := <-n.inputChan
	var res Msg = &numMsg{
		num: a,
	}
	return []Msg{res, res}
}

func (n *nodeB) Name() string {
	return "NodeB"
}

func (n *nodeB) Operate(in []Msg) []Msg {
	if len(in) != 1 {
		panic("illegal in")
	}
	a, ok := in[0].(*numMsg)
	if !ok {
		return []Msg{}
	}
	b := math.Pow(a.num, 2)
	var res Msg = &numMsg{
		num: b,
	}
	return []Msg{res}
}

func (n *nodeC) Name() string {
	return "NodeC"
}

func (n *nodeC) Operate(in []Msg) []Msg {
	if len(in) != 1 {
		panic("illegal in")
	}
	a, ok := in[0].(*numMsg)
	if !ok {
		return []Msg{}
	}
	c := math.Sqrt(a.num)
	var res Msg = &numMsg{
		num: c,
	}
	return []Msg{res}
}

func (n *nodeD) Name() string {
	return "NodeD"
}

func (n *nodeD) Operate(in []Msg) []Msg {
	if len(in) != 2 {
		panic("illegal in")
	}
	b, ok := in[0].(*numMsg)
	if !ok {
		return nil
	}
	c, ok := in[1].(*numMsg)
	if !ok {
		return nil
	}
	d := b.num + c.num
	n.outputChan <- d
	// return nil because nodeD doesn't have any downstream node.
	return nil
}

func createExampleFlowGraph() (*TimeTickedFlowGraph, chan float64, chan float64, context.CancelFunc) {
	const MaxQueueLength = 1024

	ctx, cancel := context.WithCancel(context.Background())
	inputChan := make(chan float64, MaxQueueLength)
	outputChan := make(chan float64, MaxQueueLength)

	fg := NewTimeTickedFlowGraph(ctx)

	var a Node = &nodeA{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
		inputChan: inputChan,
	}
	var b Node = &nodeB{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
	}
	var c Node = &nodeC{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
	}
	var d Node = &nodeD{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
		outputChan: outputChan,
	}

	fg.AddNode(a)
	fg.AddNode(b)
	fg.AddNode(c)
	fg.AddNode(d)

	var err = fg.SetEdges(a.Name(),
		[]string{},
		[]string{b.Name(), c.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed")
	}

	err = fg.SetEdges(b.Name(),
		[]string{a.Name()},
		[]string{d.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed")
	}

	err = fg.SetEdges(c.Name(),
		[]string{a.Name()},
		[]string{d.Name()},
	)
	if err != nil {
		log.Fatal("set edges failed")
	}

	err = fg.SetEdges(d.Name(),
		[]string{b.Name(), c.Name()},
		[]string{},
	)
	if err != nil {
		log.Fatal("set edges failed")
	}

	return fg, inputChan, outputChan, cancel
}

func TestTimeTickedFlowGraph_AddNode(t *testing.T) {
	const MaxQueueLength = 1024
	inputChan := make(chan float64, MaxQueueLength)

	fg := NewTimeTickedFlowGraph(context.TODO())

	var a Node = &nodeA{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
		inputChan: inputChan,
	}
	var b Node = &nodeB{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
	}

	fg.AddNode(a)
	assert.Equal(t, len(fg.nodeCtx), 1)
	fg.AddNode(b)
	assert.Equal(t, len(fg.nodeCtx), 2)
}

func TestTimeTickedFlowGraph_SetEdges(t *testing.T) {
	const MaxQueueLength = 1024
	inputChan := make(chan float64, MaxQueueLength)

	fg := NewTimeTickedFlowGraph(context.TODO())

	var a Node = &nodeA{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
		inputChan: inputChan,
	}
	var b Node = &nodeB{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
	}
	var c Node = &nodeC{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
	}

	fg.AddNode(a)
	fg.AddNode(b)
	fg.AddNode(c)

	var err = fg.SetEdges(a.Name(),
		[]string{b.Name()},
		[]string{c.Name()},
	)
	assert.Nil(t, err)

	err = fg.SetEdges("Invalid",
		[]string{b.Name()},
		[]string{c.Name()},
	)
	assert.Error(t, err)

	err = fg.SetEdges(a.Name(),
		[]string{"Invalid"},
		[]string{c.Name()},
	)
	assert.Error(t, err)

	err = fg.SetEdges(a.Name(),
		[]string{b.Name()},
		[]string{"Invalid"},
	)
	assert.Error(t, err)
}

func TestTimeTickedFlowGraph_Start(t *testing.T) {
	fg, inputChan, outputChan, cancel := createExampleFlowGraph()
	defer cancel()
	go fg.Start()

	// input
	time.Sleep(10 * time.Millisecond)
	go func() {
		for i := 0; i < 10; i++ {
			a := float64(rand.Int())
			inputChan <- a

			// output check
			d := <-outputChan
			res := math.Pow(a, 2) + math.Sqrt(a)
			assert.Equal(t, d, res)
		}
	}()
	time.Sleep(50 * time.Millisecond)
}

func TestTimeTickedFlowGraph_Close(t *testing.T) {
	fg, _, _, cancel := createExampleFlowGraph()
	defer cancel()
	fg.Close()
}
