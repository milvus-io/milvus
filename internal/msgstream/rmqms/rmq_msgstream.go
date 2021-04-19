package rmqms

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gogo/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	client "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
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
type Producer = client.Producer
type Consumer = client.Consumer

type RmqMsgStream struct {
	ctx              context.Context
	client           client.Client
	producers        map[string]Producer
	producerChannels []string
	consumers        map[string]Consumer
	consumerChannels []string
	unmarshal        msgstream.UnmarshalDispatcher
	repackFunc       msgstream.RepackFunc

	receiveBuf       chan *MsgPack
	wait             *sync.WaitGroup
	streamCancel     func()
	rmqBufSize       int64
	producerLock     *sync.Mutex
	consumerLock     *sync.Mutex
	consumerReflects []reflect.SelectCase

	scMap *sync.Map
}

func newRmqMsgStream(ctx context.Context, receiveBufSize int64, rmqBufSize int64,
	unmarshal msgstream.UnmarshalDispatcher) (*RmqMsgStream, error) {

	streamCtx, streamCancel := context.WithCancel(ctx)
	producers := make(map[string]Producer)
	producerChannels := make([]string, 0)
	consumerReflects := make([]reflect.SelectCase, 0)
	consumers := make(map[string]Consumer)
	consumerChannels := make([]string, 0)
	receiveBuf := make(chan *MsgPack, receiveBufSize)

	var clientOpts client.ClientOptions
	client, err := client.NewClient(clientOpts)
	if err != nil {
		defer streamCancel()
		log.Error("Set rmq client failed, error", zap.Error(err))
		return nil, err
	}

	stream := &RmqMsgStream{
		ctx:              streamCtx,
		client:           client,
		producers:        producers,
		producerChannels: producerChannels,
		consumers:        consumers,
		consumerChannels: consumerChannels,
		unmarshal:        unmarshal,
		receiveBuf:       receiveBuf,
		streamCancel:     streamCancel,
		consumerReflects: consumerReflects,
		producerLock:     &sync.Mutex{},
		consumerLock:     &sync.Mutex{},
		wait:             &sync.WaitGroup{},
		scMap:            &sync.Map{},
	}

	return stream, nil
}

func (rms *RmqMsgStream) Start() {
}

func (rms *RmqMsgStream) Close() {
	rms.streamCancel()
	rms.wait.Wait()

	if rms.client != nil {
		rms.client.Close()
	}
}

func (rms *RmqMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	rms.repackFunc = repackFunc
}

func (rms *RmqMsgStream) AsProducer(channels []string) {
	for _, channel := range channels {
		pp, err := rms.client.CreateProducer(client.ProducerOptions{Topic: channel})
		if err == nil {
			rms.producerLock.Lock()
			rms.producers[channel] = pp
			rms.producerChannels = append(rms.producerChannels, channel)
			rms.producerLock.Unlock()
		} else {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (rms *RmqMsgStream) AsConsumer(channels []string, groupName string) {
	for _, channel := range channels {
		if _, ok := rms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			receiveChannel := make(chan client.ConsumerMessage, rms.rmqBufSize)
			pc, err := rms.client.Subscribe(client.ConsumerOptions{
				Topic:            channel,
				SubscriptionName: groupName,
				MessageChannel:   receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("RocksMQ is not ready, consumer is nil")
			}

			rms.consumers[channel] = pc
			rms.consumerChannels = append(rms.consumerChannels, channel)
			rms.consumerReflects = append(rms.consumerReflects, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(pc.Chan()),
			})
			rms.wait.Add(1)
			go rms.receiveMsg(pc)
			return nil
		}
		err := util.Retry(20, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (rms *RmqMsgStream) Produce(ctx context.Context, pack *msgstream.MsgPack) error {
	tsMsgs := pack.Msgs
	if len(tsMsgs) <= 0 {
		log.Debug("Warning: Receive empty msgPack")
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
		channel := rms.producerChannels[k]
		for i := 0; i < len(v.Msgs); i++ {
			mb, err := v.Msgs[i].Marshal(v.Msgs[i])
			if err != nil {
				return err
			}

			m, err := msgstream.ConvertToByteArray(mb)
			if err != nil {
				return err
			}
			msg := &client.ProducerMessage{Payload: m}
			if err := rms.producers[channel].Send(msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rms *RmqMsgStream) Broadcast(ctx context.Context, msgPack *MsgPack) error {
	for _, v := range msgPack.Msgs {
		mb, err := v.Marshal(v)
		if err != nil {
			return err
		}

		m, err := msgstream.ConvertToByteArray(mb)
		if err != nil {
			return err
		}

		msg := &client.ProducerMessage{Payload: m}

		rms.producerLock.Lock()
		for _, producer := range rms.producers {
			if err := producer.Send(
				msg,
			); err != nil {
				return err
			}
		}
		rms.producerLock.Unlock()
	}
	return nil
}

func (rms *RmqMsgStream) Consume() (*msgstream.MsgPack, context.Context) {
	for {
		select {
		case cm, ok := <-rms.receiveBuf:
			if !ok {
				log.Debug("buf chan closed")
				return nil, nil
			}
			return cm, nil
		case <-rms.ctx.Done():
			//log.Debug("context closed")
			return nil, nil
		}
	}
}

/**
receiveMsg func is used to solve search timeout problem
which is caused by selectcase
*/
func (rms *RmqMsgStream) receiveMsg(consumer Consumer) {
	defer rms.wait.Done()

	for {
		select {
		case <-rms.ctx.Done():
			return
		case rmqMsg, ok := <-consumer.Chan():
			if !ok {
				return
			}
			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(rmqMsg.Payload, &headerMsg)
			if err != nil {
				log.Error("Failed to unmarshal message header", zap.Error(err))
				continue
			}
			tsMsg, err := rms.unmarshal.Unmarshal(rmqMsg.Payload, headerMsg.Base.MsgType)
			if err != nil {
				log.Error("Failed to unmarshal tsMsg", zap.Error(err))
				continue
			}

			tsMsg.SetPosition(&msgstream.MsgPosition{
				ChannelName: filepath.Base(consumer.Topic()),
				MsgID:       strconv.Itoa(int(rmqMsg.MsgID)),
			})

			msgPack := MsgPack{Msgs: []TsMsg{tsMsg}}
			rms.receiveBuf <- &msgPack
		}
	}
}

func (rms *RmqMsgStream) Chan() <-chan *msgstream.MsgPack {
	return rms.receiveBuf
}

func (rms *RmqMsgStream) Seek(mp *msgstream.MsgPosition) error {
	if _, ok := rms.consumers[mp.ChannelName]; ok {
		consumer := rms.consumers[mp.ChannelName]
		msgID, err := strconv.ParseInt(mp.MsgID, 10, 64)
		if err != nil {
			return err
		}
		err = consumer.Seek(msgID)
		if err != nil {
			return err
		}
		return nil
	}

	return errors.New("msgStream seek fail")
}

type RmqTtMsgStream struct {
	RmqMsgStream
	unsolvedBuf   map[Consumer][]TsMsg
	msgPositions  map[Consumer]*internalpb.MsgPosition
	unsolvedMutex *sync.Mutex
	lastTimeStamp Timestamp
	syncConsumer  chan int
}

func newRmqTtMsgStream(ctx context.Context, receiveBufSize int64, rmqBufSize int64,
	unmarshal msgstream.UnmarshalDispatcher) (*RmqTtMsgStream, error) {
	rmqMsgStream, err := newRmqMsgStream(ctx, receiveBufSize, rmqBufSize, unmarshal)
	if err != nil {
		return nil, err
	}

	unsolvedBuf := make(map[Consumer][]TsMsg)
	syncConsumer := make(chan int, 1)
	msgPositions := make(map[Consumer]*internalpb.MsgPosition)

	return &RmqTtMsgStream{
		RmqMsgStream:  *rmqMsgStream,
		unsolvedBuf:   unsolvedBuf,
		msgPositions:  msgPositions,
		unsolvedMutex: &sync.Mutex{},
		syncConsumer:  syncConsumer,
	}, nil
}

func (rtms *RmqTtMsgStream) addConsumer(consumer Consumer, channel string) {
	if len(rtms.consumers) == 0 {
		rtms.syncConsumer <- 1
	}
	rtms.consumers[channel] = consumer
	rtms.unsolvedBuf[consumer] = make([]TsMsg, 0)
	rtms.msgPositions[consumer] = &internalpb.MsgPosition{
		ChannelName: channel,
		MsgID:       "",
		Timestamp:   rtms.lastTimeStamp,
	}
	rtms.consumerChannels = append(rtms.consumerChannels, channel)
}

func (rtms *RmqTtMsgStream) AsConsumer(channels []string,
	groupName string) {
	for _, channel := range channels {
		if _, ok := rtms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			receiveChannel := make(chan client.ConsumerMessage, rtms.rmqBufSize)
			pc, err := rtms.client.Subscribe(client.ConsumerOptions{
				Topic:            channel,
				SubscriptionName: groupName,
				MessageChannel:   receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("RocksMQ is not ready, consumer is nil")
			}

			rtms.consumerLock.Lock()
			rtms.addConsumer(pc, channel)
			rtms.consumerLock.Unlock()
			return nil
		}
		err := util.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (rtms *RmqTtMsgStream) Start() {
	if rtms.consumers != nil {
		rtms.wait.Add(1)
		go rtms.bufMsgPackToChannel()
	}
}

func (rtms *RmqTtMsgStream) Close() {
	rtms.streamCancel()
	close(rtms.syncConsumer)
	rtms.wait.Wait()

	if rtms.client != nil {
		rtms.client.Close()
	}
}

func (rtms *RmqTtMsgStream) bufMsgPackToChannel() {
	defer rtms.wait.Done()
	rtms.unsolvedBuf = make(map[Consumer][]TsMsg)
	isChannelReady := make(map[Consumer]bool)
	eofMsgTimeStamp := make(map[Consumer]Timestamp)

	if _, ok := <-rtms.syncConsumer; !ok {
		log.Debug("consumer closed!")
		return
	}

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
			rtms.consumerLock.Unlock()
			wg.Wait()
			timeStamp, ok := checkTimeTickMsg(eofMsgTimeStamp, isChannelReady, &findMapMutex)
			if !ok || timeStamp <= rtms.lastTimeStamp {
				//log.Printf("All timeTick's timestamps are inconsistent")
				continue
			}
			timeTickBuf := make([]TsMsg, 0)
			startMsgPosition := make([]*internalpb.MsgPosition, 0)
			endMsgPositions := make([]*internalpb.MsgPosition, 0)
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

				startMsgPosition = append(startMsgPosition, rtms.msgPositions[consumer])
				var newPos *internalpb.MsgPosition
				if len(tempBuffer) > 0 {
					newPos = &internalpb.MsgPosition{
						ChannelName: tempBuffer[0].Position().ChannelName,
						MsgID:       tempBuffer[0].Position().MsgID,
						Timestamp:   timeStamp,
					}
					endMsgPositions = append(endMsgPositions, newPos)
				} else {
					newPos = &internalpb.MsgPosition{
						ChannelName: timeTickMsg.Position().ChannelName,
						MsgID:       timeTickMsg.Position().MsgID,
						Timestamp:   timeStamp,
					}
					endMsgPositions = append(endMsgPositions, newPos)
				}
				rtms.msgPositions[consumer] = newPos
			}
			rtms.unsolvedMutex.Unlock()

			msgPack := MsgPack{
				BeginTs:        rtms.lastTimeStamp,
				EndTs:          timeStamp,
				Msgs:           timeTickBuf,
				StartPositions: startMsgPosition,
				EndPositions:   endMsgPositions,
			}

			rtms.receiveBuf <- &msgPack
			rtms.lastTimeStamp = timeStamp
		}
	}
}

func (rtms *RmqTtMsgStream) findTimeTick(consumer Consumer,
	eofMsgMap map[Consumer]Timestamp,
	wg *sync.WaitGroup,
	findMapMutex *sync.RWMutex) {
	defer wg.Done()
	for {
		select {
		case <-rtms.ctx.Done():
			return
		case rmqMsg, ok := <-consumer.Chan():
			if !ok {
				log.Debug("consumer closed!")
				return
			}

			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(rmqMsg.Payload, &headerMsg)
			if err != nil {
				log.Error("Failed to unmarshal message header", zap.Error(err))
				continue
			}
			tsMsg, err := rtms.unmarshal.Unmarshal(rmqMsg.Payload, headerMsg.Base.MsgType)
			if err != nil {
				log.Error("Failed to unmarshal tsMsg", zap.Error(err))
				continue
			}

			tsMsg.SetPosition(&msgstream.MsgPosition{
				ChannelName: filepath.Base(consumer.Topic()),
				MsgID:       strconv.Itoa(int(rmqMsg.MsgID)),
			})

			rtms.unsolvedMutex.Lock()
			rtms.unsolvedBuf[consumer] = append(rtms.unsolvedBuf[consumer], tsMsg)
			rtms.unsolvedMutex.Unlock()

			if headerMsg.Base.MsgType == commonpb.MsgType_TimeTick {
				findMapMutex.Lock()
				eofMsgMap[consumer] = tsMsg.(*TimeTickMsg).Base.Timestamp
				findMapMutex.Unlock()
				return
			}
		}
	}
}

func (rtms *RmqTtMsgStream) Seek(mp *msgstream.MsgPosition) error {
	if len(mp.MsgID) == 0 {
		return errors.New("when msgID's length equal to 0, please use AsConsumer interface")
	}
	var consumer Consumer
	var err error
	var hasWatched bool
	seekChannel := mp.ChannelName
	subName := mp.MsgGroup
	rtms.consumerLock.Lock()
	defer rtms.consumerLock.Unlock()
	consumer, hasWatched = rtms.consumers[seekChannel]

	if hasWatched {
		return errors.New("the channel should has been subscribed")
	}

	receiveChannel := make(chan client.ConsumerMessage, rtms.rmqBufSize)
	consumer, err = rtms.client.Subscribe(client.ConsumerOptions{
		Topic:            seekChannel,
		SubscriptionName: subName,
		MessageChannel:   receiveChannel,
	})
	if err != nil {
		return err
	}
	if consumer == nil {
		return errors.New("RocksMQ is not ready, consumer is nil")
	}
	seekMsgID, err := strconv.ParseInt(mp.MsgID, 10, 64)
	if err != nil {
		return err
	}
	consumer.Seek(seekMsgID)
	rtms.addConsumer(consumer, seekChannel)

	if len(consumer.Chan()) == 0 {
		return nil
	}

	for {
		select {
		case <-rtms.ctx.Done():
			return nil
		case rmqMsg, ok := <-consumer.Chan():
			if !ok {
				return errors.New("consumer closed")
			}

			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(rmqMsg.Payload, &headerMsg)
			if err != nil {
				log.Error("Failed to unmarshal message header", zap.Error(err))
			}
			tsMsg, err := rtms.unmarshal.Unmarshal(rmqMsg.Payload, headerMsg.Base.MsgType)
			if err != nil {
				log.Error("Failed to unmarshal tsMsg", zap.Error(err))
			}
			if tsMsg.Type() == commonpb.MsgType_TimeTick {
				if tsMsg.BeginTs() >= mp.Timestamp {
					return nil
				}
				continue
			}
			if tsMsg.BeginTs() > mp.Timestamp {
				tsMsg.SetPosition(&msgstream.MsgPosition{
					ChannelName: filepath.Base(consumer.Topic()),
					MsgID:       strconv.Itoa(int(rmqMsg.MsgID)),
				})
				rtms.unsolvedBuf[consumer] = append(rtms.unsolvedBuf[consumer], tsMsg)
			}
		}
	}
}

func checkTimeTickMsg(msg map[Consumer]Timestamp,
	isChannelReady map[Consumer]bool,
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
