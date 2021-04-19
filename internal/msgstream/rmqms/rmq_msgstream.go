package rmqms

import (
	"context"
	"errors"
	"log"
	"reflect"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	rocksmq "github.com/zilliztech/milvus-distributed/internal/util/rocksmq"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type RmqMsgStream struct {
	isServing        int64
	ctx              context.Context
	serverLoopWg     sync.WaitGroup
	serverLoopCtx    context.Context
	serverLoopCancel func()

	rmq        *rocksmq.RocksMQ
	repackFunc msgstream.RepackFunc
	consumers  []rocksmq.Consumer
	producers  []string

	unmarshal  msgstream.UnmarshalDispatcher
	receiveBuf chan *msgstream.MsgPack
	wait       *sync.WaitGroup
	// tso ticker
	streamCancel func()
}

func NewRmqMsgStream(ctx context.Context, rmq *rocksmq.RocksMQ, receiveBufSize int64) *RmqMsgStream {
	streamCtx, streamCancel := context.WithCancel(ctx)
	receiveBuf := make(chan *msgstream.MsgPack, receiveBufSize)
	stream := &RmqMsgStream{
		ctx:          streamCtx,
		rmq:          nil,
		receiveBuf:   receiveBuf,
		streamCancel: streamCancel,
	}

	return stream
}

func (ms *RmqMsgStream) Start() {
	ms.wait = &sync.WaitGroup{}
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *RmqMsgStream) Close() {
}

func (ms *RmqMsgStream) CreateProducers(channels []string) error {
	for _, channel := range channels {
		// TODO(yhz): Here may allow to create an existing channel
		if err := ms.rmq.CreateChannel(channel); err != nil {
			return err
		}
	}
	return nil
}

func (ms *RmqMsgStream) CreateConsumers(channels []string, groupName string) error {
	for _, channelName := range channels {
		if err := ms.rmq.CreateConsumerGroup(groupName, channelName); err != nil {
			return err
		}
		msgNum := make(chan int)
		ms.consumers = append(ms.consumers, rocksmq.Consumer{GroupName: groupName, ChannelName: channelName, MsgNum: msgNum})
	}
	return nil
}

func (ms *RmqMsgStream) Produce(pack *msgstream.MsgPack) error {
	tsMsgs := pack.Msgs
	if len(tsMsgs) <= 0 {
		log.Printf("Warning: Receive empty msgPack")
		return nil
	}
	if len(ms.producers) <= 0 {
		return errors.New("nil producer in msg stream")
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	for channelID, tsMsg := range tsMsgs {
		hashValues := tsMsg.HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			if tsMsg.Type() == commonpb.MsgType_kSearchResult {
				searchResult := tsMsg.(*msgstream.SearchResultMsg)
				channelID := searchResult.ResultChannelID
				channelIDInt, _ := strconv.ParseInt(channelID, 10, 64)
				if channelIDInt >= int64(len(ms.producers)) {
					return errors.New("Failed to produce pulsar msg to unKnow channel")
				}
				bucketValues[index] = int32(channelIDInt)
				continue
			}
			bucketValues[index] = int32(hashValue % uint32(len(ms.producers)))
		}
		reBucketValues[channelID] = bucketValues
	}

	var result map[int32]*msgstream.MsgPack
	var err error
	if ms.repackFunc != nil {
		result, err = ms.repackFunc(tsMsgs, reBucketValues)
	} else {
		msgType := (tsMsgs[0]).Type()
		switch msgType {
		case commonpb.MsgType_kInsert:
			result, err = util.InsertRepackFunc(tsMsgs, reBucketValues)
		case commonpb.MsgType_kDelete:
			result, err = util.DeleteRepackFunc(tsMsgs, reBucketValues)
		default:
			result, err = util.DefaultRepackFunc(tsMsgs, reBucketValues)
		}
	}
	if err != nil {
		return err
	}
	for k, v := range result {
		for i := 0; i < len(v.Msgs); i++ {
			mb, err := v.Msgs[i].Marshal(v.Msgs[i])
			if err != nil {
				return err
			}
			//
			//msg := &pulsar.ProducerMessage{Payload: mb}

			//var child opentracing.Span
			if v.Msgs[i].Type() == commonpb.MsgType_kInsert ||
				v.Msgs[i].Type() == commonpb.MsgType_kSearch ||
				v.Msgs[i].Type() == commonpb.MsgType_kSearchResult {
				//tracer := opentracing.GlobalTracer()
				//ctx := v.Msgs[i].GetMsgContext()
				//if ctx == nil {
				//	ctx = context.Background()
				//}
				//
				//if parent := opentracing.SpanFromContext(ctx); parent != nil {
				//	child = tracer.StartSpan("start send pulsar msg",
				//		opentracing.FollowsFrom(parent.Context()))
				//} else {
				//	child = tracer.StartSpan("start send pulsar msg")
				//}
				//child.SetTag("hash keys", v.Msgs[i].HashKeys())
				//child.SetTag("start time", v.Msgs[i].BeginTs())
				//child.SetTag("end time", v.Msgs[i].EndTs())
				//child.SetTag("msg type", v.Msgs[i].Type())
				//msg.Properties = make(map[string]string)
				//err = tracer.Inject(child.Context(), opentracing.TextMap, &propertiesReaderWriter{msg.Properties})
				//if err != nil {
				//	child.LogFields(oplog.Error(err))
				//	child.Finish()
				//	return err
				//}
				//child.LogFields(oplog.String("inject success", "inject success"))
			}

			m, err := msgstream.ConvertToByteArray(mb)
			if err != nil {
				return err
			}
			msg := make([]rocksmq.ProducerMessage, 0)
			msg = append(msg, *rocksmq.NewProducerMessage(m))
			if err := ms.rmq.Produce(ms.producers[k], msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ms *RmqMsgStream) Consume() *msgstream.MsgPack {
	for {
		select {
		case cm, ok := <-ms.receiveBuf:
			if !ok {
				log.Println("buf chan closed")
				return nil
			}
			return cm
		case <-ms.ctx.Done():
			log.Printf("context closed")
			return nil
		}
	}
}

func (ms *RmqMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()

	cases := make([]reflect.SelectCase, len(ms.consumers))
	for i := 0; i < len(ms.consumers); i++ {
		ch := ms.consumers[i].MsgNum
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	for {
		select {
		case <-ms.ctx.Done():
			log.Println("done")
			return
		default:
			tsMsgList := make([]msgstream.TsMsg, 0)

			for {
				chosen, value, ok := reflect.Select(cases)
				if !ok {
					log.Printf("channel closed")
					return
				}

				msgNum := value.Interface().(int)
				rmqMsg, err := ms.rmq.Consume(ms.consumers[chosen].GroupName, ms.consumers[chosen].ChannelName, msgNum)
				if err != nil {
					log.Printf("Failed to consume message in rocksmq, error = %v", err)
					continue
				}
				for j := 0; j < len(rmqMsg); j++ {
					headerMsg := commonpb.MsgHeader{}
					err := proto.Unmarshal(rmqMsg[j].Payload, &headerMsg)
					if err != nil {
						log.Printf("Failed to unmarshal message header, error = %v", err)
						continue
					}
					tsMsg, err := ms.unmarshal.Unmarshal(rmqMsg[j].Payload, headerMsg.Base.MsgType)
					if err != nil {
						log.Printf("Failed to unmarshal tsMsg, error = %v", err)
						continue
					}
					tsMsgList = append(tsMsgList, tsMsg)
				}
				noMoreMessage := true
				for k := 0; k < len(ms.consumers); k++ {
					if len(ms.consumers[k].MsgNum) > 0 {
						noMoreMessage = false
					}
				}

				if noMoreMessage {
					break
				}
			}

			if len(tsMsgList) > 0 {
				msgPack := util.MsgPack{Msgs: tsMsgList}
				ms.receiveBuf <- &msgPack
			}
		}
	}
}

func (ms *RmqMsgStream) Chan() <-chan *msgstream.MsgPack {
	return nil
}
