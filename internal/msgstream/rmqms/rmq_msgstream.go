package rmqms

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
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

type RmqMsgStream struct {
	isServing int64
	ctx       context.Context

	repackFunc       msgstream.RepackFunc
	consumers        []rocksmq.Consumer
	consumerChannels []string
	producers        []string

	unmarshal  msgstream.UnmarshalDispatcher
	receiveBuf chan *msgstream.MsgPack
	wait       *sync.WaitGroup
	// tso ticker
	streamCancel     func()
	rmqBufSize       int64
	consumerLock     *sync.Mutex
	consumerReflects []reflect.SelectCase
}

func newRmqMsgStream(ctx context.Context, receiveBufSize int64, rmqBufSize int64,
	unmarshal msgstream.UnmarshalDispatcher) (*RmqMsgStream, error) {

	streamCtx, streamCancel := context.WithCancel(ctx)
	receiveBuf := make(chan *msgstream.MsgPack, receiveBufSize)
	consumerChannels := make([]string, 0)
	consumerReflects := make([]reflect.SelectCase, 0)
	consumers := make([]rocksmq.Consumer, 0)
	stream := &RmqMsgStream{
		ctx:              streamCtx,
		receiveBuf:       receiveBuf,
		unmarshal:        unmarshal,
		streamCancel:     streamCancel,
		rmqBufSize:       rmqBufSize,
		consumers:        consumers,
		consumerChannels: consumerChannels,
		consumerReflects: consumerReflects,
		consumerLock:     &sync.Mutex{},
		wait:             &sync.WaitGroup{},
	}

	return stream, nil
}

func (rms *RmqMsgStream) Start() {
}

func (rms *RmqMsgStream) Close() {
	rms.streamCancel()

	for _, consumer := range rms.consumers {
		_ = rocksmq.Rmq.DestroyConsumerGroup(consumer.GroupName, consumer.ChannelName)
		close(consumer.MsgMutex)
	}
}

type propertiesReaderWriter struct {
	ppMap map[string]string
}

func (rms *RmqMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	rms.repackFunc = repackFunc
}

func (rms *RmqMsgStream) AsProducer(channels []string) {
	for _, channel := range channels {
		err := rocksmq.Rmq.CreateChannel(channel)
		if err == nil {
			rms.producers = append(rms.producers, channel)
		} else {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (rms *RmqMsgStream) AsConsumer(channels []string, groupName string) {
	for _, channelName := range channels {
		consumer, err := rocksmq.Rmq.CreateConsumerGroup(groupName, channelName)
		if err == nil {
			consumer.MsgMutex = make(chan struct{}, rms.rmqBufSize)
			//consumer.MsgMutex <- struct{}{}
			rms.consumers = append(rms.consumers, *consumer)
			rms.consumerChannels = append(rms.consumerChannels, channelName)
			rms.consumerReflects = append(rms.consumerReflects, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(consumer.MsgMutex),
			})
			rms.wait.Add(1)
			go rms.receiveMsg(*consumer)
		}
	}
}

func (rms *RmqMsgStream) Produce(ctx context.Context, pack *msgstream.MsgPack) error {
	tsMsgs := pack.Msgs
	if len(tsMsgs) <= 0 {
		log.Printf("Warning: Receive empty msgPack")
		return nil
	}
	if len(rms.producers) <= 0 {
		return errors.New("nil producer in msg stream")
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	for channelID, tsMsg := range tsMsgs {
		hashValues := tsMsg.HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			if tsMsg.Type() == commonpb.MsgType_SearchResult {
				searchResult := tsMsg.(*msgstream.SearchResultMsg)
				channelID := searchResult.ResultChannelID
				channelIDInt, _ := strconv.ParseInt(channelID, 10, 64)
				if channelIDInt >= int64(len(rms.producers)) {
					return errors.New("Failed to produce rmq msg to unKnow channel")
				}
				bucketValues[index] = int32(channelIDInt)
				continue
			}
			bucketValues[index] = int32(hashValue % uint32(len(rms.producers)))
		}
		reBucketValues[channelID] = bucketValues
	}

	var result map[int32]*msgstream.MsgPack
	var err error
	if rms.repackFunc != nil {
		result, err = rms.repackFunc(tsMsgs, reBucketValues)
	} else {
		msgType := (tsMsgs[0]).Type()
		switch msgType {
		case commonpb.MsgType_Insert:
			result, err = util.InsertRepackFunc(tsMsgs, reBucketValues)
		case commonpb.MsgType_Delete:
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

			m, err := msgstream.ConvertToByteArray(mb)
			if err != nil {
				return err
			}
			msg := make([]rocksmq.ProducerMessage, 0)
			msg = append(msg, *rocksmq.NewProducerMessage(m))

			if err := rocksmq.Rmq.Produce(rms.producers[k], msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rms *RmqMsgStream) Broadcast(ctx context.Context, msgPack *MsgPack) error {
	producerLen := len(rms.producers)
	for _, v := range msgPack.Msgs {
		mb, err := v.Marshal(v)
		if err != nil {
			return err
		}
		m, err := msgstream.ConvertToByteArray(mb)
		if err != nil {
			return err
		}
		msg := make([]rocksmq.ProducerMessage, 0)
		msg = append(msg, *rocksmq.NewProducerMessage(m))

		for i := 0; i < producerLen; i++ {
			if err := rocksmq.Rmq.Produce(rms.producers[i], msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rms *RmqMsgStream) Consume() (*msgstream.MsgPack, context.Context) {
	for {
		select {
		case cm, ok := <-rms.receiveBuf:
			if !ok {
				log.Println("buf chan closed")
				return nil, nil
			}
			return cm, nil
		case <-rms.ctx.Done():
			log.Printf("context closed")
			return nil, nil
		}
	}
}

/**
receiveMsg func is used to solve search timeout problem
which is caused by selectcase
*/
func (rms *RmqMsgStream) receiveMsg(consumer rocksmq.Consumer) {
	defer rms.wait.Done()

	for {
		select {
		case <-rms.ctx.Done():
			return
		case _, ok := <-consumer.MsgMutex:
			if !ok {
				return
			}
			tsMsgList := make([]msgstream.TsMsg, 0)
			for {
				rmqMsgs, err := rocksmq.Rmq.Consume(consumer.GroupName, consumer.ChannelName, 1)
				if err != nil {
					log.Printf("Failed to consume message in rocksmq, error = %v", err)
					continue
				}
				if len(rmqMsgs) == 0 {
					break
				}
				rmqMsg := rmqMsgs[0]
				headerMsg := commonpb.MsgHeader{}
				err = proto.Unmarshal(rmqMsg.Payload, &headerMsg)
				if err != nil {
					log.Printf("Failed to unmar`shal message header, error = %v", err)
					continue
				}
				tsMsg, err := rms.unmarshal.Unmarshal(rmqMsg.Payload, headerMsg.Base.MsgType)
				if err != nil {
					log.Printf("Failed to unmarshal tsMsg, error = %v", err)
					continue
				}
				tsMsgList = append(tsMsgList, tsMsg)
			}

			if len(tsMsgList) > 0 {
				msgPack := util.MsgPack{Msgs: tsMsgList}
				rms.receiveBuf <- &msgPack
			}
		}
	}
}

func (rms *RmqMsgStream) Chan() <-chan *msgstream.MsgPack {
	return rms.receiveBuf
}

func (rms *RmqMsgStream) Seek(offset *msgstream.MsgPosition) error {
	for i := 0; i < len(rms.consumers); i++ {
		if rms.consumers[i].ChannelName == offset.ChannelName {
			messageID, err := strconv.ParseInt(offset.MsgID, 10, 64)
			if err != nil {
				return err
			}
			err = rocksmq.Rmq.Seek(rms.consumers[i].GroupName, rms.consumers[i].ChannelName, messageID)
			if err != nil {
				return err
			}
			return nil
		}
	}

	return errors.New("msgStream seek fail")
}

type RmqTtMsgStream struct {
	RmqMsgStream
	unsolvedBuf   map[rocksmq.Consumer][]TsMsg
	unsolvedMutex *sync.Mutex
	lastTimeStamp Timestamp
}

func newRmqTtMsgStream(ctx context.Context, receiveBufSize int64, rmqBufSize int64,
	unmarshal msgstream.UnmarshalDispatcher) (*RmqTtMsgStream, error) {
	rmqMsgStream, err := newRmqMsgStream(ctx, receiveBufSize, rmqBufSize, unmarshal)
	if err != nil {
		return nil, err
	}
	unsolvedBuf := make(map[rocksmq.Consumer][]TsMsg)
	return &RmqTtMsgStream{
		RmqMsgStream:  *rmqMsgStream,
		unsolvedBuf:   unsolvedBuf,
		unsolvedMutex: &sync.Mutex{},
	}, nil
}

func (rtms *RmqTtMsgStream) AsConsumer(channels []string,
	groupName string) {
	for _, channelName := range channels {
		consumer, err := rocksmq.Rmq.CreateConsumerGroup(groupName, channelName)
		if err != nil {
			panic(err.Error())
		}
		consumer.MsgMutex = make(chan struct{}, rtms.rmqBufSize)
		//consumer.MsgMutex <- struct{}{}
		rtms.consumers = append(rtms.consumers, *consumer)
		rtms.consumerChannels = append(rtms.consumerChannels, consumer.ChannelName)
		rtms.consumerReflects = append(rtms.consumerReflects, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(consumer.MsgMutex),
		})
	}
}

func (rtms *RmqTtMsgStream) Start() {
	rtms.wait = &sync.WaitGroup{}
	if rtms.consumers != nil {
		rtms.wait.Add(1)
		go rtms.bufMsgPackToChannel()
	}
}

func (rtms *RmqTtMsgStream) bufMsgPackToChannel() {
	defer rtms.wait.Done()
	rtms.unsolvedBuf = make(map[rocksmq.Consumer][]TsMsg)
	isChannelReady := make(map[rocksmq.Consumer]bool)
	eofMsgTimeStamp := make(map[rocksmq.Consumer]Timestamp)

	for {
		select {
		case <-rtms.ctx.Done():
			return
		default:
			wg := sync.WaitGroup{}
			findMapMutex := sync.RWMutex{}
			rtms.consumerLock.Lock()
			for _, consumer := range rtms.consumers {
				if isChannelReady[consumer] {
					continue
				}
				wg.Add(1)
				go rtms.findTimeTick(consumer, eofMsgTimeStamp, &wg, &findMapMutex)
			}
			wg.Wait()
			timeStamp, ok := checkTimeTickMsg(eofMsgTimeStamp, isChannelReady, &findMapMutex)
			rtms.consumerLock.Unlock()
			if !ok || timeStamp <= rtms.lastTimeStamp {
				//log.Printf("All timeTick's timestamps are inconsistent")
				continue
			}
			timeTickBuf := make([]TsMsg, 0)
			msgPositions := make([]*msgstream.MsgPosition, 0)
			rtms.unsolvedMutex.Lock()
			for consumer, msgs := range rtms.unsolvedBuf {
				if len(msgs) == 0 {
					continue
				}
				tempBuffer := make([]TsMsg, 0)
				var timeTickMsg TsMsg
				for _, v := range msgs {
					if v.Type() == commonpb.MsgType_TimeTick {
						timeTickMsg = v
						continue
					}
					if v.EndTs() <= timeStamp {
						timeTickBuf = append(timeTickBuf, v)
					} else {
						tempBuffer = append(tempBuffer, v)
					}
				}
				rtms.unsolvedBuf[consumer] = tempBuffer

				if len(tempBuffer) > 0 {
					msgPositions = append(msgPositions, &msgstream.MsgPosition{
						ChannelName: tempBuffer[0].Position().ChannelName,
						MsgID:       tempBuffer[0].Position().MsgID,
						Timestamp:   timeStamp,
					})
				} else {
					msgPositions = append(msgPositions, &msgstream.MsgPosition{
						ChannelName: timeTickMsg.Position().ChannelName,
						MsgID:       timeTickMsg.Position().MsgID,
						Timestamp:   timeStamp,
					})
				}
			}
			rtms.unsolvedMutex.Unlock()

			msgPack := MsgPack{
				BeginTs:        rtms.lastTimeStamp,
				EndTs:          timeStamp,
				Msgs:           timeTickBuf,
				StartPositions: msgPositions,
			}

			rtms.receiveBuf <- &msgPack
			rtms.lastTimeStamp = timeStamp
		}
	}
}

func (rtms *RmqTtMsgStream) findTimeTick(consumer rocksmq.Consumer,
	eofMsgMap map[rocksmq.Consumer]Timestamp,
	wg *sync.WaitGroup,
	findMapMutex *sync.RWMutex) {
	defer wg.Done()
	for {
		select {
		case <-rtms.ctx.Done():
			return
		case _, ok := <-consumer.MsgMutex:
			if !ok {
				log.Printf("consumer closed!")
				return
			}
			for {
				rmqMsgs, err := rocksmq.Rmq.Consume(consumer.GroupName, consumer.ChannelName, 1)
				if err != nil {
					log.Printf("Failed to consume message in rocksmq, error = %v", err)
					continue
				}
				if len(rmqMsgs) == 0 {
					return
				}
				rmqMsg := rmqMsgs[0]
				headerMsg := commonpb.MsgHeader{}
				err = proto.Unmarshal(rmqMsg.Payload, &headerMsg)
				if err != nil {
					log.Printf("Failed to unmarshal message header, error = %v", err)
					continue
				}
				tsMsg, err := rtms.unmarshal.Unmarshal(rmqMsg.Payload, headerMsg.Base.MsgType)
				if err != nil {
					log.Printf("Failed to unmarshal tsMsg, error = %v", err)
					continue
				}

				tsMsg.SetPosition(&msgstream.MsgPosition{
					ChannelName: filepath.Base(consumer.ChannelName),
					MsgID:       strconv.Itoa(int(rmqMsg.MsgID)),
				})

				rtms.unsolvedMutex.Lock()
				rtms.unsolvedBuf[consumer] = append(rtms.unsolvedBuf[consumer], tsMsg)
				rtms.unsolvedMutex.Unlock()

				if headerMsg.Base.MsgType == commonpb.MsgType_TimeTick {
					findMapMutex.Lock()
					eofMsgMap[consumer] = tsMsg.(*TimeTickMsg).Base.Timestamp
					findMapMutex.Unlock()
					//consumer.MsgMutex <- struct{}{}
					//return
				}
			}
		}
	}
}

func (rtms *RmqTtMsgStream) Seek(mp *msgstream.MsgPosition) error {
	var consumer rocksmq.Consumer
	var msgID UniqueID
	for index, channel := range rtms.consumerChannels {
		if filepath.Base(channel) == filepath.Base(mp.ChannelName) {
			consumer = rtms.consumers[index]
			if len(mp.MsgID) == 0 {
				msgID = -1
				break
			}
			seekMsgID, err := strconv.ParseInt(mp.MsgID, 10, 64)
			if err != nil {
				return err
			}
			msgID = UniqueID(seekMsgID)
			break
		}
	}
	err := rocksmq.Rmq.Seek(consumer.GroupName, consumer.ChannelName, msgID)
	if err != nil {
		return err
	}
	if msgID == -1 {
		return nil
	}
	rtms.unsolvedMutex.Lock()
	rtms.unsolvedBuf[consumer] = make([]TsMsg, 0)

	// When rmq seek is called, msgMutex can't be used before current msgs all consumed, because
	// new msgMutex is not generated. So just try to consume msgs
	for {
		rmqMsg, err := rocksmq.Rmq.Consume(consumer.GroupName, consumer.ChannelName, 1)
		if err != nil {
			log.Printf("Failed to consume message in rocksmq, error = %v", err)
		}
		if len(rmqMsg) == 0 {
			break
		} else {
			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(rmqMsg[0].Payload, &headerMsg)
			if err != nil {
				log.Printf("Failed to unmarshal message header, error = %v", err)
				return err
			}
			tsMsg, err := rtms.unmarshal.Unmarshal(rmqMsg[0].Payload, headerMsg.Base.MsgType)
			if err != nil {
				log.Printf("Failed to unmarshal tsMsg, error = %v", err)
				return err
			}

			if headerMsg.Base.MsgType == commonpb.MsgType_TimeTick {
				if tsMsg.BeginTs() >= mp.Timestamp {
					rtms.unsolvedMutex.Unlock()
					return nil
				}
				continue
			}
			if tsMsg.BeginTs() > mp.Timestamp {
				tsMsg.SetPosition(&msgstream.MsgPosition{
					ChannelName: filepath.Base(consumer.ChannelName),
					MsgID:       strconv.Itoa(int(rmqMsg[0].MsgID)),
				})
				rtms.unsolvedBuf[consumer] = append(rtms.unsolvedBuf[consumer], tsMsg)
			}
		}
	}
	return nil

	//for {
	//	select {
	//	case <-rtms.ctx.Done():
	//		return nil
	//	case num, ok := <-consumer.MsgNum:
	//		if !ok {
	//			return errors.New("consumer closed")
	//		}
	//		rmqMsg, err := rocksmq.Rmq.Consume(consumer.GroupName, consumer.ChannelName, num)
	//		if err != nil {
	//			log.Printf("Failed to consume message in rocksmq, error = %v", err)
	//			continue
	//		}
	//
	//		for j := 0; j < len(rmqMsg); j++ {
	//			headerMsg := commonpb.MsgHeader{}
	//			err := proto.Unmarshal(rmqMsg[j].Payload, &headerMsg)
	//			if err != nil {
	//				log.Printf("Failed to unmarshal message header, error = %v", err)
	//			}
	//			tsMsg, err := rtms.unmarshal.Unmarshal(rmqMsg[j].Payload, headerMsg.Base.MsgType)
	//			if err != nil {
	//				log.Printf("Failed to unmarshal tsMsg, error = %v", err)
	//			}
	//
	//			if headerMsg.Base.MsgType == commonpb.MsgType_kTimeTick {
	//				if tsMsg.BeginTs() >= mp.Timestamp {
	//					rtms.unsolvedMutex.Unlock()
	//					return nil
	//				}
	//				continue
	//			}
	//			if tsMsg.BeginTs() > mp.Timestamp {
	//				tsMsg.SetPosition(&msgstream.MsgPosition{
	//					ChannelName: filepath.Base(consumer.ChannelName),
	//					MsgID:       strconv.Itoa(int(rmqMsg[j].MsgID)),
	//				})
	//				rtms.unsolvedBuf[consumer] = append(rtms.unsolvedBuf[consumer], tsMsg)
	//			}
	//		}
	//	}
	//}
}

func checkTimeTickMsg(msg map[rocksmq.Consumer]Timestamp,
	isChannelReady map[rocksmq.Consumer]bool,
	mu *sync.RWMutex) (Timestamp, bool) {
	checkMap := make(map[Timestamp]int)
	var maxTime Timestamp = 0
	for _, v := range msg {
		checkMap[v]++
		if v > maxTime {
			maxTime = v
		}
	}
	if len(checkMap) <= 1 {
		for consumer := range msg {
			isChannelReady[consumer] = false
		}
		return maxTime, true
	}
	for consumer := range msg {
		mu.RLock()
		v := msg[consumer]
		mu.RUnlock()
		if v != maxTime {
			isChannelReady[consumer] = false
		} else {
			isChannelReady[consumer] = true
		}
	}

	return 0, false
}
