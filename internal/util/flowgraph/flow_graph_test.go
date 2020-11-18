package flowgraph

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const ctxTimeInMillisecond = 3000

type nodeA struct {
	BaseNode
	a float64
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
	d       float64
	resChan chan float64
}

type intMsg struct {
	num float64
	t   Timestamp
}

func (m *intMsg) TimeTick() Timestamp {
	return m.t
}

func (m *intMsg) DownStreamNodeIdx() int {
	return 1
}

func intMsg2Msg(in []*intMsg) []*Msg {
	out := make([]*Msg, 0)
	for _, msg := range in {
		var m Msg = msg
		out = append(out, &m)
	}
	return out
}

func msg2IntMsg(in []*Msg) []*intMsg {
	out := make([]*intMsg, 0)
	for _, msg := range in {
		out = append(out, (*msg).(*intMsg))
	}
	return out
}

func (a *nodeA) Name() string {
	return "NodeA"
}

func (a *nodeA) Operate(in []*Msg) []*Msg {
	return append(in, in...)
}

func (b *nodeB) Name() string {
	return "NodeB"
}

func (b *nodeB) Operate(in []*Msg) []*Msg {
	messages := make([]*intMsg, 0)
	for _, msg := range msg2IntMsg(in) {
		messages = append(messages, &intMsg{
			num: math.Pow(msg.num, 2),
		})
	}
	return intMsg2Msg(messages)
}

func (c *nodeC) Name() string {
	return "NodeC"
}

func (c *nodeC) Operate(in []*Msg) []*Msg {
	messages := make([]*intMsg, 0)
	for _, msg := range msg2IntMsg(in) {
		messages = append(messages, &intMsg{
			num: math.Sqrt(msg.num),
		})
	}
	return intMsg2Msg(messages)
}

func (d *nodeD) Name() string {
	return "NodeD"
}

func (d *nodeD) Operate(in []*Msg) []*Msg {
	messages := make([]*intMsg, 0)
	outLength := len(in) / 2
	inMessages := msg2IntMsg(in)
	for i := 0; i < outLength; i++ {
		var msg = &intMsg{
			num: inMessages[i].num + inMessages[i+outLength].num,
		}
		messages = append(messages, msg)
	}
	d.d = messages[0].num
	d.resChan <- d.d
	fmt.Println("flow graph result:", d.d)
	return intMsg2Msg(messages)
}

func sendMsgFromCmd(ctx context.Context, fg *TimeTickedFlowGraph) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Millisecond * time.Duration(500))
			var num = float64(rand.Int() % 100)
			var msg Msg = &intMsg{num: num}
			a := nodeA{}
			fg.nodeCtx[a.Name()].inputChannels[0] <- &msg
			fmt.Println("send number", num, "to node", a.Name())
			res, ok := receiveResult(ctx, fg)
			if !ok {
				return
			}
			// assert result
			expect := math.Pow(num, 2) + math.Sqrt(num)
			if big.NewFloat(res) != big.NewFloat(expect) {
				fmt.Println(res)
				fmt.Println(math.Pow(num, 2) + math.Sqrt(num))
				panic("wrong answer")
			}
		}
	}
}

func receiveResultFromNodeD(res *float64, fg *TimeTickedFlowGraph, wg *sync.WaitGroup) {
	d := nodeD{}
	node := fg.nodeCtx[d.Name()]
	nd, ok := (*node.node).(*nodeD)
	if !ok {
		log.Fatal("not nodeD type")
	}
	*res = <-nd.resChan
	wg.Done()
}

func receiveResult(ctx context.Context, fg *TimeTickedFlowGraph) (float64, bool) {
	d := nodeD{}
	node := fg.nodeCtx[d.Name()]
	nd, ok := (*node.node).(*nodeD)
	if !ok {
		log.Fatal("not nodeD type")
	}
	select {
	case <-ctx.Done():
		return 0, false
	case res := <-nd.resChan:
		return res, true
	}
}

func TestTimeTickedFlowGraph_Start(t *testing.T) {
	const MaxQueueLength = 1024
	const MaxParallelism = 1024

	duration := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), duration)
	defer cancel()

	fg := NewTimeTickedFlowGraph(ctx)

	var a Node = &nodeA{
		BaseNode: BaseNode{
			maxQueueLength: MaxQueueLength,
		},
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
		resChan: make(chan float64),
	}

	fg.AddNode(&a)
	fg.AddNode(&b)
	fg.AddNode(&c)
	fg.AddNode(&d)

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

	// init node A
	nodeCtxA := fg.nodeCtx[a.Name()]
	nodeCtxA.inputChannels = []chan *Msg{make(chan *Msg, 10)}

	go fg.Start()

	sendMsgFromCmd(ctx, fg)
}
