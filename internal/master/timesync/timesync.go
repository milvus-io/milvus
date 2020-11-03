package timesync

import (
	"context"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

const stopReadFlagId int64 = -1

type TimeTickReader struct {
	pulsarClient pulsar.Client

	timeTickConsumer pulsar.Consumer
	readerProducer   []pulsar.Producer

	interval    int64
	proxyIdList []int64

	timeTickPeerProxy map[int64]uint64
	ctx               context.Context
}

func (r *TimeTickReader) Start() {
	go r.readTimeTick()
	go r.timeSync()
}

func (r *TimeTickReader) Close() {
	if r.timeTickConsumer != nil {
		r.timeTickConsumer.Close()
	}

	for i := 0; i < len(r.readerProducer); i++ {
		if r.readerProducer[i] != nil {
			r.readerProducer[i].Close()
		}
	}
	if r.pulsarClient != nil {
		r.pulsarClient.Close()
	}
}

func (r *TimeTickReader) timeSync() {
	ctx := r.ctx
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Millisecond * time.Duration(r.interval))
			var minTimeStamp uint64
			for _, minTimeStamp = range r.timeTickPeerProxy {
				break
			}
			for _, ts := range r.timeTickPeerProxy {
				if ts < minTimeStamp {
					minTimeStamp = ts
				}
			}
			//send timestamp flag to reader channel
			msg := pb.InsertOrDeleteMsg{Timestamp: minTimeStamp, ClientId: stopReadFlagId}
			payload, err := proto.Marshal(&msg)
			if err != nil {
				//TODO log error
				log.Printf("Marshal InsertOrDeleteMsg flag error %v", err)
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
}

func (r *TimeTickReader) readTimeTick() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case cm, ok := <-r.timeTickConsumer.Chan():
			if ok == false {
				log.Printf("timesync consumer closed")
			}

			msg := cm.Message
			var tsm pb.TimeSyncMsg
			if err := proto.Unmarshal(msg.Payload(), &tsm); err != nil {
				log.Printf("UnMarshal timetick flag error  %v", err)
			}

			r.timeTickPeerProxy[tsm.Peer_Id] = tsm.Timestamp
			r.timeTickConsumer.AckID(msg.ID())
		}
	}
}

func (r *TimeTickReader) sendEOFMsg(ctx context.Context, msg *pulsar.ProducerMessage, index int, wg *sync.WaitGroup) {
	if _, err := r.readerProducer[index].Send(ctx, msg); err != nil {
		log.Printf("Send timesync flag error %v", err)
	}
	wg.Done()
}

func TimeTickService() {
	timeTickTopic := "timeTick"
	timeTickSubName := "master"
	readTopics := make([]string, 0)
	for i := conf.Config.Reader.TopicStart; i < conf.Config.Reader.TopicEnd; i++ {
		str := "InsertOrDelete-"
		str = str + strconv.Itoa(i)
		readTopics = append(readTopics, str)
	}

	proxyIdList := conf.Config.Master.ProxyIdList
	timeTickReader := newTimeTickReader(context.Background(), timeTickTopic, timeTickSubName, readTopics, proxyIdList)
	timeTickReader.Start()
}

func newTimeTickReader(
	ctx context.Context,
	timeTickTopic string,
	timeTickSubName string,
	readTopics []string,
	proxyIdList []int64,
) *TimeTickReader {
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	interval := int64(conf.Config.Timesync.Interval)

	//check if proxyId has duplication
	if len(proxyIdList) == 0 {
		log.Printf("proxy id list is empty")
	}
	if len(proxyIdList) > 1 {
		sort.Slice(proxyIdList, func(i int, j int) bool { return proxyIdList[i] < proxyIdList[j] })
	}
	for i := 1; i < len(proxyIdList); i++ {
		if proxyIdList[i] == proxyIdList[i-1] {
			log.Printf("there are two proxies have the same id = %d", proxyIdList[i])
		}
	}
	r := TimeTickReader{}
	r.interval = interval
	r.proxyIdList = proxyIdList
	readerQueueSize := conf.Config.Reader.ReaderQueueSize

	//check if read topic is empty
	if len(readTopics) == 0 {
		log.Printf("read topic is empyt")
	}
	//set default value
	if readerQueueSize == 0 {
		readerQueueSize = 1024
	}

	r.timeTickPeerProxy = make(map[int64]uint64)
	r.ctx = ctx

	var client pulsar.Client
	var err error
	if conf.Config.Pulsar.Authentication {
		client, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:            pulsarAddr,
			Authentication: pulsar.NewAuthenticationToken(conf.Config.Pulsar.Token),
		})
	} else {
		client, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddr})
	}

	if err != nil {
		log.Printf("connect pulsar failed, %v", err)
	}
	r.pulsarClient = client

	timeSyncChan := make(chan pulsar.ConsumerMessage, len(r.proxyIdList))
	if r.timeTickConsumer, err = r.pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       timeTickTopic,
		SubscriptionName:            timeTickSubName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              timeSyncChan,
	}); err != nil {
		log.Printf("failed to subscribe topic %s, error = %v", timeTickTopic, err)
	}

	r.readerProducer = make([]pulsar.Producer, 0, len(readTopics))
	for i := 0; i < len(readTopics); i++ {
		rp, err := r.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readTopics[i]})
		if err != nil {
			log.Printf("failed to create reader producer %s, error = %v", readTopics[i], err)
		}
		r.readerProducer = append(r.readerProducer, rp)
	}

	return &r
}
