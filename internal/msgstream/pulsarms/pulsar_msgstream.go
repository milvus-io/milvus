package pulsarms

import (
	"context"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type TsMsg = msgstream.TsMsg
type MsgPack = msgstream.MsgPack
type MsgType = msgstream.MsgType
type UniqueID = msgstream.UniqueID
type BaseMsg = msgstream.BaseMsg
type Timestamp = msgstream.Timestamp
type IntPrimaryKey = msgstream.IntPrimaryKey
type TimeTickMsg = msgstream.TimeTickMsg
type QueryNodeStatsMsg = msgstream.QueryNodeStatsMsg
type RepackFunc = msgstream.RepackFunc

type PulsarMsgStream struct {
	ctx          context.Context
	client       *pulsar.Client
	producers    []*pulsar.Producer
	consumers    []*pulsar.Consumer
	repackFunc   RepackFunc
	unmarshal    *util.UnmarshalDispatcher
	receiveBuf   chan *MsgPack
	wait         *sync.WaitGroup
	streamCancel func()
}

func NewPulsarMsgStream(ctx context.Context, receiveBufSize int64) *PulsarMsgStream {
	streamCtx, streamCancel := context.WithCancel(ctx)
	stream := &PulsarMsgStream{
		ctx:          streamCtx,
		streamCancel: streamCancel,
	}
	stream.receiveBuf = make(chan *MsgPack, receiveBufSize)
	return stream
}

func (ms *PulsarMsgStream) SetPulsarClient(address string) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: address})
	if err != nil {
		log.Printf("Set pulsar client failed, error = %v", err)
	}
	ms.client = &client
}

func (ms *PulsarMsgStream) CreatePulsarProducers(channels []string) {
	for i := 0; i < len(channels); i++ {
		fn := func() error {
			pp, err := (*ms.client).CreateProducer(pulsar.ProducerOptions{Topic: channels[i]})
			if err != nil {
				return err
			}
			if pp == nil {
				return errors.New("pulsar is not ready, producer is nil")
			}
			ms.producers = append(ms.producers, &pp)
			return nil
		}
		err := util.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create producer " + channels[i] + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *PulsarMsgStream) CreatePulsarConsumers(channels []string,
	subName string,
	unmarshal *util.UnmarshalDispatcher,
	pulsarBufSize int64) {
	ms.unmarshal = unmarshal
	for i := 0; i < len(channels); i++ {
		fn := func() error {
			receiveChannel := make(chan pulsar.ConsumerMessage, pulsarBufSize)
			pc, err := (*ms.client).Subscribe(pulsar.ConsumerOptions{
				Topic:                       channels[i],
				SubscriptionName:            subName,
				Type:                        pulsar.KeyShared,
				SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("pulsar is not ready, consumer is nil")
			}
			ms.consumers = append(ms.consumers, &pc)
			return nil
		}
		err := util.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channels[i] + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *PulsarMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *PulsarMsgStream) Start() {
	ms.wait = &sync.WaitGroup{}
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *PulsarMsgStream) Close() {
	ms.streamCancel()

	for _, producer := range ms.producers {
		if producer != nil {
			(*producer).Close()
		}
	}
	for _, consumer := range ms.consumers {
		if consumer != nil {
			(*consumer).Close()
		}
	}
	if ms.client != nil {
		(*ms.client).Close()
	}
}

type propertiesReaderWriter struct {
	ppMap map[string]string
}

func (ppRW *propertiesReaderWriter) Set(key, val string) {
	// The GRPC HPACK implementation rejects any uppercase keys here.
	//
	// As such, since the HTTP_HEADERS format is case-insensitive anyway, we
	// blindly lowercase the key (which is guaranteed to work in the
	// Inject/Extract sense per the OpenTracing spec).
	key = strings.ToLower(key)
	ppRW.ppMap[key] = val
}

func (ppRW *propertiesReaderWriter) ForeachKey(handler func(key, val string) error) error {
	for k, val := range ppRW.ppMap {
		if err := handler(k, val); err != nil {
			return err
		}
	}
	return nil
}

func (ms *PulsarMsgStream) Produce(msgPack *MsgPack) error {
	tsMsgs := msgPack.Msgs
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

	var result map[int32]*MsgPack
	var err error
	if ms.repackFunc != nil {
		result, err = ms.repackFunc(tsMsgs, reBucketValues)
	} else {
		msgType := (tsMsgs[0]).Type()
		switch msgType {
		case commonpb.MsgType_kInsert:
			result, err = msgstream.InsertRepackFunc(tsMsgs, reBucketValues)
		case commonpb.MsgType_kDelete:
			result, err = msgstream.DeleteRepackFunc(tsMsgs, reBucketValues)
		default:
			result, err = msgstream.DefaultRepackFunc(tsMsgs, reBucketValues)
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

			msg := &pulsar.ProducerMessage{Payload: mb}

			var child opentracing.Span
			if v.Msgs[i].Type() == commonpb.MsgType_kInsert ||
				v.Msgs[i].Type() == commonpb.MsgType_kSearch ||
				v.Msgs[i].Type() == commonpb.MsgType_kSearchResult {
				tracer := opentracing.GlobalTracer()
				ctx := v.Msgs[i].GetMsgContext()
				if ctx == nil {
					ctx = context.Background()
				}

				if parent := opentracing.SpanFromContext(ctx); parent != nil {
					child = tracer.StartSpan("start send pulsar msg",
						opentracing.FollowsFrom(parent.Context()))
				} else {
					child = tracer.StartSpan("start send pulsar msg")
				}
				child.SetTag("hash keys", v.Msgs[i].HashKeys())
				child.SetTag("start time", v.Msgs[i].BeginTs())
				child.SetTag("end time", v.Msgs[i].EndTs())
				child.SetTag("msg type", v.Msgs[i].Type())
				msg.Properties = make(map[string]string)
				err = tracer.Inject(child.Context(), opentracing.TextMap, &propertiesReaderWriter{msg.Properties})
				if err != nil {
					child.LogFields(oplog.Error(err))
					child.Finish()
					return err
				}
				child.LogFields(oplog.String("inject success", "inject success"))
			}

			if _, err := (*ms.producers[k]).Send(
				context.Background(),
				msg,
			); err != nil {
				if child != nil {
					child.LogFields(oplog.Error(err))
					child.Finish()
				}
				return err
			}
			if child != nil {
				child.Finish()
			}
		}
	}
	return nil
}

func (ms *PulsarMsgStream) Broadcast(msgPack *MsgPack) error {
	producerLen := len(ms.producers)
	for _, v := range msgPack.Msgs {
		mb, err := v.Marshal(v)
		if err != nil {
			return err
		}
		msg := &pulsar.ProducerMessage{Payload: mb}
		var child opentracing.Span
		if v.Type() == commonpb.MsgType_kInsert ||
			v.Type() == commonpb.MsgType_kSearch ||
			v.Type() == commonpb.MsgType_kSearchResult {
			tracer := opentracing.GlobalTracer()
			ctx := v.GetMsgContext()
			if ctx == nil {
				ctx = context.Background()
			}
			if parent := opentracing.SpanFromContext(ctx); parent != nil {
				child = tracer.StartSpan("start send pulsar msg",
					opentracing.FollowsFrom(parent.Context()))
			} else {
				child = tracer.StartSpan("start send pulsar msg, start time: %d")
			}
			child.SetTag("hash keys", v.HashKeys())
			child.SetTag("start time", v.BeginTs())
			child.SetTag("end time", v.EndTs())
			child.SetTag("msg type", v.Type())
			msg.Properties = make(map[string]string)
			err = tracer.Inject(child.Context(), opentracing.TextMap, &propertiesReaderWriter{msg.Properties})
			if err != nil {
				child.LogFields(oplog.Error(err))
				child.Finish()
				return err
			}
			child.LogFields(oplog.String("inject success", "inject success"))
		}
		for i := 0; i < producerLen; i++ {
			if _, err := (*ms.producers[i]).Send(
				context.Background(),
				msg,
			); err != nil {
				if child != nil {
					child.LogFields(oplog.Error(err))
					child.Finish()
				}
				return err
			}
		}
		if child != nil {
			child.Finish()
		}
	}
	return nil
}

func (ms *PulsarMsgStream) Consume() *MsgPack {
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

func (ms *PulsarMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()

	cases := make([]reflect.SelectCase, len(ms.consumers))
	for i := 0; i < len(ms.consumers); i++ {
		ch := (*ms.consumers[i]).Chan()
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	for {
		select {
		case <-ms.ctx.Done():
			log.Println("done")
			return
		default:
			tsMsgList := make([]TsMsg, 0)

			for {
				chosen, value, ok := reflect.Select(cases)
				if !ok {
					log.Printf("channel closed")
					return
				}

				pulsarMsg, ok := value.Interface().(pulsar.ConsumerMessage)

				if !ok {
					log.Printf("type assertion failed, not consumer message type")
					continue
				}
				(*ms.consumers[chosen]).AckID(pulsarMsg.ID())

				headerMsg := commonpb.MsgHeader{}
				err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
				if err != nil {
					log.Printf("Failed to unmarshal message header, error = %v", err)
					continue
				}
				tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.Base.MsgType)
				if tsMsg.Type() == commonpb.MsgType_kSearch ||
					tsMsg.Type() == commonpb.MsgType_kSearchResult {
					tracer := opentracing.GlobalTracer()
					spanContext, err := tracer.Extract(opentracing.HTTPHeaders, &propertiesReaderWriter{pulsarMsg.Properties()})
					if err != nil {
						log.Println("extract message err")
						log.Println(err.Error())
					}
					span := opentracing.StartSpan("pulsar msg received",
						ext.RPCServerOption(spanContext))
					span.SetTag("msg type", tsMsg.Type())
					span.SetTag("hash keys", tsMsg.HashKeys())
					span.SetTag("start time", tsMsg.BeginTs())
					span.SetTag("end time", tsMsg.EndTs())
					tsMsg.SetMsgContext(opentracing.ContextWithSpan(context.Background(), span))
					span.Finish()
				}
				if err != nil {
					log.Printf("Failed to unmarshal tsMsg, error = %v", err)
					continue
				}
				tsMsgList = append(tsMsgList, tsMsg)

				noMoreMessage := true
				for i := 0; i < len(ms.consumers); i++ {
					if len((*ms.consumers[i]).Chan()) > 0 {
						noMoreMessage = false
					}
				}

				if noMoreMessage {
					break
				}
			}

			if len(tsMsgList) > 0 {
				msgPack := MsgPack{Msgs: tsMsgList}
				ms.receiveBuf <- &msgPack
			}
		}
	}
}

func (ms *PulsarMsgStream) Chan() <-chan *MsgPack {
	return ms.receiveBuf
}

type PulsarTtMsgStream struct {
	PulsarMsgStream
	inputBuf      []TsMsg
	unsolvedBuf   []TsMsg
	lastTimeStamp Timestamp
}

func NewPulsarTtMsgStream(ctx context.Context, receiveBufSize int64) *PulsarTtMsgStream {
	streamCtx, streamCancel := context.WithCancel(ctx)
	pulsarMsgStream := PulsarMsgStream{
		ctx:          streamCtx,
		streamCancel: streamCancel,
	}
	pulsarMsgStream.receiveBuf = make(chan *MsgPack, receiveBufSize)
	return &PulsarTtMsgStream{
		PulsarMsgStream: pulsarMsgStream,
	}
}

func (ms *PulsarTtMsgStream) Start() {
	ms.wait = &sync.WaitGroup{}
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *PulsarTtMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()
	ms.unsolvedBuf = make([]TsMsg, 0)
	ms.inputBuf = make([]TsMsg, 0)
	isChannelReady := make([]bool, len(ms.consumers))
	eofMsgTimeStamp := make(map[int]Timestamp)
	spans := make(map[Timestamp]opentracing.Span)
	ctxs := make(map[Timestamp]context.Context)
	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			wg := sync.WaitGroup{}
			mu := sync.Mutex{}
			findMapMutex := sync.RWMutex{}
			for i := 0; i < len(ms.consumers); i++ {
				if isChannelReady[i] {
					continue
				}
				wg.Add(1)
				go ms.findTimeTick(i, eofMsgTimeStamp, &wg, &mu, &findMapMutex)
			}
			wg.Wait()
			timeStamp, ok := msgstream.CheckTimeTickMsg(eofMsgTimeStamp, isChannelReady, &findMapMutex)
			if !ok || timeStamp <= ms.lastTimeStamp {
				log.Printf("All timeTick's timestamps are inconsistent")
				continue
			}

			timeTickBuf := make([]TsMsg, 0)
			ms.inputBuf = append(ms.inputBuf, ms.unsolvedBuf...)
			ms.unsolvedBuf = ms.unsolvedBuf[:0]
			for _, v := range ms.inputBuf {
				var ctx context.Context
				var span opentracing.Span
				if v.Type() == commonpb.MsgType_kInsert {
					if _, ok := spans[v.BeginTs()]; !ok {
						span, ctx = opentracing.StartSpanFromContext(v.GetMsgContext(), "after find time tick")
						ctxs[v.BeginTs()] = ctx
						spans[v.BeginTs()] = span
					}
				}
				if v.EndTs() <= timeStamp {
					timeTickBuf = append(timeTickBuf, v)
					if v.Type() == commonpb.MsgType_kInsert {
						v.SetMsgContext(ctxs[v.BeginTs()])
						spans[v.BeginTs()].Finish()
						delete(spans, v.BeginTs())
					}
				} else {
					ms.unsolvedBuf = append(ms.unsolvedBuf, v)
				}
			}
			ms.inputBuf = ms.inputBuf[:0]

			msgPack := MsgPack{
				BeginTs: ms.lastTimeStamp,
				EndTs:   timeStamp,
				Msgs:    timeTickBuf,
			}

			ms.receiveBuf <- &msgPack
			ms.lastTimeStamp = timeStamp
		}
	}
}

func (ms *PulsarTtMsgStream) findTimeTick(channelIndex int,
	eofMsgMap map[int]Timestamp,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	findMapMutex *sync.RWMutex) {
	defer wg.Done()
	for {
		select {
		case <-ms.ctx.Done():
			return
		case pulsarMsg, ok := <-(*ms.consumers[channelIndex]).Chan():
			if !ok {
				log.Printf("consumer closed!")
				return
			}
			(*ms.consumers[channelIndex]).Ack(pulsarMsg)

			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
			if err != nil {
				log.Printf("Failed to unmarshal, error = %v", err)
			}
			unMarshalFunc := (*ms.unmarshal).TempMap[headerMsg.Base.MsgType]
			if unMarshalFunc == nil {
				panic("null unMarshalFunc for " + headerMsg.Base.MsgType.String() + " msg type")
			}
			tsMsg, err := unMarshalFunc(pulsarMsg.Payload())
			if err != nil {
				log.Printf("Failed to unmarshal, error = %v", err)
			}

			if tsMsg.Type() == commonpb.MsgType_kInsert {
				tracer := opentracing.GlobalTracer()
				spanContext, err := tracer.Extract(opentracing.HTTPHeaders, &propertiesReaderWriter{pulsarMsg.Properties()})
				if err != nil {
					log.Println("extract message err")
					log.Println(err.Error())
				}
				span := opentracing.StartSpan("pulsar msg received",
					ext.RPCServerOption(spanContext))
				span.SetTag("hash keys", tsMsg.HashKeys())
				span.SetTag("start time", tsMsg.BeginTs())
				span.SetTag("end time", tsMsg.EndTs())
				span.SetTag("msg type", tsMsg.Type())
				tsMsg.SetMsgContext(opentracing.ContextWithSpan(context.Background(), span))
				span.Finish()
			}

			if headerMsg.Base.MsgType == commonpb.MsgType_kTimeTick {
				findMapMutex.Lock()
				eofMsgMap[channelIndex] = tsMsg.(*TimeTickMsg).Base.Timestamp
				findMapMutex.Unlock()
				return
			}
			mu.Lock()
			ms.inputBuf = append(ms.inputBuf, tsMsg)
			mu.Unlock()
		}
	}
}
