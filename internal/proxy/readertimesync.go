package proxy

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"log"
	"sort"
	"strconv"
	"sync"
)

const ReadStopFlagEnd int64 = 0

type ReaderTimeSync interface {
	Start() error
	Close()
	TimeSync() <-chan TimeSyncMsg
	ManipulationReqMsg() <-chan *pb.ManipulationReqMsg
	IsManipulationReqMsgChanFull() bool
}

type TimeSyncMsg struct {
	Timestamp    uint64
	NumRecorders int64
}

type ReaderTimeSyncOption func(*ReaderTimeSyncCfg)

type ReaderTimeSyncCfg struct {
	pulsarAddr   string
	pulsarClient pulsar.Client

	timeSyncConsumer pulsar.Consumer
	readerConsumer   pulsar.Consumer
	readerProducer   []pulsar.Producer

	timesyncMsgChan        chan TimeSyncMsg
	manipulationReqMsgChan chan *pb.ManipulationReqMsg //output insert or delete msg

	readStopFlagClientId int64
	interval             int64
	proxyIdList          []int64
	readerQueueSize      int

	revTimesyncFromReader map[uint64]int

	ctx    context.Context
	cancel context.CancelFunc
}

/*
layout of timestamp
   time  ms                     logic number
/-------46 bit-----------\/------18bit-----\
+-------------------------+================+
*/
func toMillisecond(ts *pb.TimeSyncMsg) int {
	// get Millisecond in second
	return int(ts.GetTimestamp() >> 18)
}

func NewReaderTimeSync(
	timeSyncTopic string,
	timeSyncSubName string,
	readTopics []string,
	readSubName string,
	proxyIdList []int64,
	readStopFlagClientId int64,
	opts ...ReaderTimeSyncOption,
) (ReaderTimeSync, error) {
	//pulsarAddr := "pulsar://"
	//pulsarAddr += conf.Config.Pulsar.Address
	//pulsarAddr += ":"
	//pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	//interval := int64(conf.Config.Timesync.Interval)

	//check if proxyId has duplication
	if len(proxyIdList) == 0 {
		return nil, fmt.Errorf("proxy id list is empty")
	}
	if len(proxyIdList) > 1 {
		sort.Slice(proxyIdList, func(i int, j int) bool { return proxyIdList[i] < proxyIdList[j] })
	}
	for i := 1; i < len(proxyIdList); i++ {
		if proxyIdList[i] == proxyIdList[i-1] {
			return nil, fmt.Errorf("there are two proxies have the same id = %d", proxyIdList[i])
		}
	}
	r := &ReaderTimeSyncCfg{
		//interval:    interval,
		proxyIdList: proxyIdList,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.interval == 0 {
		r.interval = int64(conf.Config.Timesync.Interval)
		if r.interval == 0 {
			return nil, fmt.Errorf("interval is unsetted")
		}
	}
	if len(r.pulsarAddr) == 0 {
		pulsarAddr := "pulsar://"
		pulsarAddr += conf.Config.Pulsar.Address
		pulsarAddr += ":"
		pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
		r.pulsarAddr = pulsarAddr
	}

	//check if read topic is empty
	if len(readTopics) == 0 {
		return nil, fmt.Errorf("read topic is empyt")
	}
	//set default value
	if r.readerQueueSize == 0 {
		r.readerQueueSize = 1024
	}
	if readStopFlagClientId >= ReadStopFlagEnd {
		return nil, fmt.Errorf("read stop flag client id should less than %d", ReadStopFlagEnd)
	}
	r.readStopFlagClientId = readStopFlagClientId

	r.timesyncMsgChan = make(chan TimeSyncMsg, len(readTopics)*r.readerQueueSize)
	r.manipulationReqMsgChan = make(chan *pb.ManipulationReqMsg, len(readTopics)*r.readerQueueSize)
	r.revTimesyncFromReader = make(map[uint64]int)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: r.pulsarAddr})
	if err != nil {
		return nil, fmt.Errorf("connect pulsar failed, %v", err)
	}
	r.pulsarClient = client

	timeSyncChan := make(chan pulsar.ConsumerMessage, len(r.proxyIdList))
	if r.timeSyncConsumer, err = r.pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       timeSyncTopic,
		SubscriptionName:            timeSyncSubName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              timeSyncChan,
	}); err != nil {
		return nil, fmt.Errorf("failed to subscribe topic %s, error = %v", timeSyncTopic, err)
	}

	readerChan := make(chan pulsar.ConsumerMessage, len(readTopics)*r.readerQueueSize)
	if r.readerConsumer, err = r.pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topics:                      readTopics,
		SubscriptionName:            readSubName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              readerChan,
	}); err != nil {
		return nil, fmt.Errorf("failed to subscrive reader topics : %v, error = %v", readTopics, err)
	}

	r.readerProducer = make([]pulsar.Producer, 0, len(readTopics))
	for i := 0; i < len(readTopics); i++ {
		rp, err := r.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readTopics[i]})
		if err != nil {
			return nil, fmt.Errorf("failed to create reader producer %s, error = %v", readTopics[i], err)
		}
		r.readerProducer = append(r.readerProducer, rp)
	}

	return r, nil
}

func (r *ReaderTimeSyncCfg) Close() {
	r.cancel()
	r.timeSyncConsumer.Close()
	r.readerConsumer.Close()
	for i := 0; i < len(r.readerProducer); i++ {
		r.readerProducer[i].Close()
	}
	r.pulsarClient.Close()
}

func (r *ReaderTimeSyncCfg) Start() error {
	go r.startReadTopics()
	go r.startTimeSync()
	return r.ctx.Err()
}

func (r *ReaderTimeSyncCfg) ManipulationReqMsg() <-chan *pb.ManipulationReqMsg {
	return r.manipulationReqMsgChan
}

func (r *ReaderTimeSyncCfg) TimeSync() <-chan TimeSyncMsg {
	return r.timesyncMsgChan
}

func (r *ReaderTimeSyncCfg) TimeSyncChanLen() int {
	return len(r.timesyncMsgChan)
}

func (r *ReaderTimeSyncCfg) IsManipulationReqMsgChanFull() bool {
	return len(r.manipulationReqMsgChan) == len(r.readerProducer)*r.readerQueueSize
}

func (r *ReaderTimeSyncCfg) alignTimeSync(ts []*pb.TimeSyncMsg) []*pb.TimeSyncMsg {
	if len(r.proxyIdList) > 1 {
		if len(ts) > 1 {
			for i := 1; i < len(r.proxyIdList); i++ {
				curIdx := len(ts) - 1 - i
				preIdx := len(ts) - i
				timeGap := toMillisecond(ts[curIdx]) - toMillisecond(ts[preIdx])
				if int64(timeGap) >= (r.interval/2) || int64(timeGap) <= (-r.interval/2) {
					ts = ts[preIdx:]
					return ts
				}
			}
			ts = ts[len(ts)-len(r.proxyIdList):]
			sort.Slice(ts, func(i int, j int) bool { return ts[i].Peer_Id < ts[j].Peer_Id })
			for i := 0; i < len(r.proxyIdList); i++ {
				if ts[i].Peer_Id != r.proxyIdList[i] {
					ts = ts[:0]
					return ts
				}
			}
		}
	} else {
		if len(ts) > 1 {
			ts = ts[len(ts)-1:]
		}
	}
	return ts
}

func (r *ReaderTimeSyncCfg) readTimeSync(ctx context.Context, ts []*pb.TimeSyncMsg, n int) ([]*pb.TimeSyncMsg, error) {
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case cm, ok := <-r.timeSyncConsumer.Chan():
			if ok == false {
				return nil, fmt.Errorf("timesync consumer closed")
			}

			msg := cm.Message
			var tsm pb.TimeSyncMsg
			if err := proto.Unmarshal(msg.Payload(), &tsm); err != nil {
				return nil, err
			}

			ts = append(ts, &tsm)
			r.timeSyncConsumer.AckID(msg.ID())
		}
	}
	return ts, nil
}

func (r *ReaderTimeSyncCfg) sendEOFMsg(ctx context.Context, msg *pulsar.ProducerMessage, index int, wg *sync.WaitGroup) {
	if _, err := r.readerProducer[index].Send(ctx, msg); err != nil {
		//TODO, log error
		log.Printf("Send timesync flag error %v", err)
	}
	wg.Done()
}

func (r *ReaderTimeSyncCfg) startTimeSync() {
	tsm := make([]*pb.TimeSyncMsg, 0, len(r.proxyIdList)*2)
	ctx, _ := context.WithCancel(r.ctx)
	var err error
	for {
		//var start time.Time
		for len(tsm) != len(r.proxyIdList) {
			tsm = r.alignTimeSync(tsm)
			tsm, err = r.readTimeSync(ctx, tsm, len(r.proxyIdList)-len(tsm))
			if err != nil {
				if ctx.Err() != nil {
					return
				} else {
					//TODO, log error msg
					log.Printf("read time sync error %v", err)
				}
			}
		}
		ts := tsm[0].Timestamp
		for i := 1; i < len(tsm); i++ {
			if tsm[i].Timestamp < ts {
				ts = tsm[i].Timestamp
			}
		}
		tsm = tsm[:0]
		//send timestamp flag to reader channel
		msg := pb.ManipulationReqMsg{Timestamp: ts, ProxyId: r.readStopFlagClientId}
		payload, err := proto.Marshal(&msg)
		if err != nil {
			//TODO log error
			log.Printf("Marshal timesync flag error %v", err)
		} else {
			wg := sync.WaitGroup{}
			wg.Add(len(r.readerProducer))
			for index := range r.readerProducer {
				go r.sendEOFMsg(ctx, &pulsar.ProducerMessage{Payload: payload}, index, &wg)
			}
			wg.Wait()
		}
	}
}

func (r *ReaderTimeSyncCfg) isReadStopFlag(imsg *pb.ManipulationReqMsg) bool {
	return imsg.ProxyId < ReadStopFlagEnd
}

func (r *ReaderTimeSyncCfg) startReadTopics() {
	ctx, _ := context.WithCancel(r.ctx)
	tsm := TimeSyncMsg{Timestamp: 0, NumRecorders: 0}
	for {
		select {
		case <-ctx.Done():
			return
		case cm, ok := <-r.readerConsumer.Chan():
			if ok == false {
				//TODO,log error
				log.Printf("reader consumer closed")
			}
			msg := cm.Message
			var imsg pb.ManipulationReqMsg
			if err := proto.Unmarshal(msg.Payload(), &imsg); err != nil {
				//TODO, log error
				log.Printf("unmarshal InsertOrDeleteMsg error %v", err)
				break
			}
			if r.isReadStopFlag(&imsg) { //timestamp flag
				if imsg.ProxyId == r.readStopFlagClientId {
					gval := r.revTimesyncFromReader[imsg.Timestamp]
					gval++
					if gval >= len(r.readerProducer) {
						if imsg.Timestamp >= tsm.Timestamp {
							tsm.Timestamp = imsg.Timestamp
							r.timesyncMsgChan <- tsm
							tsm.NumRecorders = 0
						}
						delete(r.revTimesyncFromReader, imsg.Timestamp)
					} else {
						r.revTimesyncFromReader[imsg.Timestamp] = gval
					}
				}
			} else {
				if r.IsManipulationReqMsgChanFull() {
					log.Printf("WARN :  Insert or delete chan is full ...")
				}
				tsm.NumRecorders++
				r.manipulationReqMsgChan <- &imsg
			}
			r.readerConsumer.AckID(msg.ID())
		}
	}
}

func WithReaderQueueSize(size int) ReaderTimeSyncOption {
	return func(r *ReaderTimeSyncCfg) {
		r.readerQueueSize = size
	}
}

func WithPulsarAddress(addr string) ReaderTimeSyncOption {
	return func(r *ReaderTimeSyncCfg) {
		r.pulsarAddr = addr
	}
}

func WithInterval(interval int64) ReaderTimeSyncOption {
	return func(r *ReaderTimeSyncCfg) {
		r.interval = interval
	}
}
