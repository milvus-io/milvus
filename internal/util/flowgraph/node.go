package flowgraph

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type Node interface {
	Name() string
	MaxQueueLength() int32
	MaxParallelism() int32
	Operate(ctx context.Context, in []Msg) ([]Msg, context.Context)
	IsInputNode() bool
}

type BaseNode struct {
	maxQueueLength int32
	maxParallelism int32
}

type nodeCtx struct {
	node                   Node
	inputChannels          []chan *MsgWithCtx
	inputMessages          []Msg
	downstream             []*nodeCtx
	downstreamInputChanIdx map[string]int

	NumActiveTasks    int64
	NumCompletedTasks int64
}

type MsgWithCtx struct {
	ctx context.Context
	msg Msg
}

func (nodeCtx *nodeCtx) Start(ctx context.Context, wg *sync.WaitGroup) {
	if nodeCtx.node.IsInputNode() {
		// fmt.Println("start InputNode.inStream")
		inStream, ok := nodeCtx.node.(*InputNode)
		if !ok {
			log.Fatal("Invalid inputNode")
		}
		(*inStream.inStream).Start()
	}

	for {
		select {
		case <-ctx.Done():
			wg.Done()
			fmt.Println(nodeCtx.node.Name(), "closed")
			return
		default:
			// inputs from inputsMessages for Operate
			inputs := make([]Msg, 0)

			var msgCtx context.Context
			var res []Msg
			var sp opentracing.Span
			if !nodeCtx.node.IsInputNode() {
				msgCtx = nodeCtx.collectInputMessages()
				inputs = nodeCtx.inputMessages
			}
			n := nodeCtx.node
			res, msgCtx = n.Operate(msgCtx, inputs)
			sp, msgCtx = trace.StartSpanFromContext(msgCtx)
			sp.SetTag("node name", n.Name())

			downstreamLength := len(nodeCtx.downstreamInputChanIdx)
			if len(nodeCtx.downstream) < downstreamLength {
				log.Println("nodeCtx.downstream length = ", len(nodeCtx.downstream))
			}
			if len(res) < downstreamLength {
				// log.Println("node result length = ", len(res))
				break
			}

			w := sync.WaitGroup{}
			for i := 0; i < downstreamLength; i++ {
				w.Add(1)
				go nodeCtx.downstream[i].ReceiveMsg(msgCtx, &w, res[i], nodeCtx.downstreamInputChanIdx[nodeCtx.downstream[i].node.Name()])
			}
			w.Wait()
			sp.Finish()
		}
	}
}

func (nodeCtx *nodeCtx) Close() {
	for _, channel := range nodeCtx.inputChannels {
		close(channel)
		fmt.Println("close inputChannel")
	}
}

func (nodeCtx *nodeCtx) ReceiveMsg(ctx context.Context, wg *sync.WaitGroup, msg Msg, inputChanIdx int) {
	sp, ctx := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	nodeCtx.inputChannels[inputChanIdx] <- &MsgWithCtx{ctx: ctx, msg: msg}
	//fmt.Println((*nodeCtx.node).Name(), "receive to input channel ", inputChanIdx)

	wg.Done()
}

func (nodeCtx *nodeCtx) collectInputMessages() context.Context {
	var opts []opentracing.StartSpanOption

	inputsNum := len(nodeCtx.inputChannels)
	nodeCtx.inputMessages = make([]Msg, inputsNum)

	// init inputMessages,
	// receive messages from inputChannels,
	// and move them to inputMessages.
	for i := 0; i < inputsNum; i++ {
		channel := nodeCtx.inputChannels[i]
		msgWithCtx, ok := <-channel
		if !ok {
			// TODO: add status
			log.Println("input channel closed")
			return nil
		}
		nodeCtx.inputMessages[i] = msgWithCtx.msg
		if msgWithCtx.ctx != nil {
			sp, _ := trace.StartSpanFromContext(msgWithCtx.ctx)
			opts = append(opts, opentracing.ChildOf(sp.Context()))
			sp.Finish()
		}
	}

	var ctx context.Context
	var sp opentracing.Span
	if len(opts) != 0 {
		sp, ctx = trace.StartSpanFromContext(context.Background(), opts...)
		defer sp.Finish()
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
					fmt.Println("try to align timestamp, t1 =", latestTime, ", t2 =", nodeCtx.inputMessages[i].TimeTick())
					channel := nodeCtx.inputChannels[i]
					msg, ok := <-channel
					if !ok {
						log.Println("input channel closed")
						return
					}
					nodeCtx.inputMessages[i] = msg.msg
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
	return ctx
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
