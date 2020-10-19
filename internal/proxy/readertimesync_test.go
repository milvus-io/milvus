package proxy

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"log"
	"sync"
	"testing"
	"time"
)

const (
	timeSyncTopic          = "rtimesync"
	timeSyncTopic2         = "rtimesync2"
	timeSyncTopic3         = "rtimesync3"
	timeSyncSubName        = "rtimesync-g"
	timeSyncSubName1       = "rtimesync-g1"
	timeSyncSubName2       = "rtimesync-g2"
	timeSyncSubName3       = "rtimesync-g3"
	readerTopic1           = "rreader1"
	readerTopic12          = "rreader12"
	readerTopic13          = "rreader13"
	readerTopic2           = "rreader2"
	readerTopic22          = "rreader22"
	readerTopic23          = "rreader23"
	readerTopic3           = "rreader3"
	readerTopic32          = "rreader32"
	readerTopic33          = "rreader33"
	readerTopic4           = "rreader4"
	readerTopic42          = "rreader42"
	readerTopic43          = "rreader43"
	readerSubName          = "rreader-g"
	readerSubName1         = "rreader-g1"
	readerSubName2         = "rreader-g2"
	readerSubName3         = "rreader-g3"
	interval               = 200
	readStopFlag     int64 = -1
	readStopFlag1    int64 = -1
	readStopFlag2    int64 = -2
	readStopFlag3    int64 = -3
)

func TestAlignTimeSync(t *testing.T) {
	r := &ReaderTimeSyncCfg{
		proxyIdList: []int64{1, 2, 3},
		interval:    200,
	}
	ts := []*pb.TimeSyncMsg{
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   3,
			Timestamp: toTimestamp(15),
		},
		{
			Peer_Id:   2,
			Timestamp: toTimestamp(20),
		},
	}
	r.alignTimeSync(ts)
	assert.Equalf(t, len(r.proxyIdList), 3, "proxyIdList should be : 1 2 3")
	for i := 0; i < len(r.proxyIdList); i++ {
		assert.Equal(t, r.proxyIdList[i], ts[i].Peer_Id)
	}

}

func TestAlignTimeSync2(t *testing.T) {
	r := &ReaderTimeSyncCfg{
		proxyIdList: []int64{1, 2, 3},
		interval:    200,
	}
	ts := []*pb.TimeSyncMsg{
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   3,
			Timestamp: toTimestamp(150),
		},
		{
			Peer_Id:   2,
			Timestamp: toTimestamp(20),
		},
	}
	ts = r.alignTimeSync(ts)
	assert.Equalf(t, len(r.proxyIdList), 3, "proxyIdList should be : 1 2 3")
	assert.Equal(t, len(ts), 1)
	assert.Equal(t, ts[0].Peer_Id, int64(2))
}

func TestAlignTimeSync3(t *testing.T) {
	r := &ReaderTimeSyncCfg{
		proxyIdList: []int64{1, 2, 3},
		interval:    200,
	}
	ts := []*pb.TimeSyncMsg{
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   3,
			Timestamp: toTimestamp(15),
		},
		{
			Peer_Id:   2,
			Timestamp: toTimestamp(20),
		},
	}
	ts = r.alignTimeSync(ts)
	assert.Equalf(t, len(r.proxyIdList), 3, "proxyIdList should be : 1 2 3")
	for i := 0; i < len(r.proxyIdList); i++ {
		assert.Equal(t, r.proxyIdList[i], ts[i].Peer_Id)
	}
}

func TestAlignTimeSync4(t *testing.T) {
	r := &ReaderTimeSyncCfg{
		proxyIdList: []int64{1},
		interval:    200,
	}
	ts := []*pb.TimeSyncMsg{
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(15),
		},
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(25),
		},
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(35),
		},
	}
	ts = r.alignTimeSync(ts)
	assert.Equalf(t, len(r.proxyIdList), 1, "proxyIdList should be : 1")
	assert.Equal(t, len(ts), 1)
	assert.Equal(t, getMillisecond(ts[0].Timestamp), uint64(35))
}

func TestAlignTimeSync5(t *testing.T) {
	r := &ReaderTimeSyncCfg{
		proxyIdList: []int64{1, 2, 3},
		interval:    200,
	}
	ts := []*pb.TimeSyncMsg{
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   1,
			Timestamp: toTimestamp(5),
		},
		{
			Peer_Id:   3,
			Timestamp: toTimestamp(15),
		},
		{
			Peer_Id:   3,
			Timestamp: toTimestamp(20),
		},
	}
	ts = r.alignTimeSync(ts)
	assert.Zero(t, len(ts))
}

func TestNewReaderTimeSync(t *testing.T) {
	r, err := NewReaderTimeSync(
		timeSyncTopic,
		timeSyncSubName,
		[]string{readerTopic1, readerTopic2, readerTopic3, readerTopic4},
		readerSubName,
		[]int64{2, 1},
		readStopFlag,
		WithPulsarAddress("pulsar://localhost:6650"),
		WithInterval(interval),
		WithReaderQueueSize(8),
	)
	assert.Nil(t, err)

	rr := r.(*ReaderTimeSyncCfg)
	assert.NotNil(t, rr.pulsarClient)
	assert.NotNil(t, rr.timeSyncConsumer)
	assert.NotNil(t, rr.readerConsumer)
	assert.NotNil(t, rr.readerProducer)
	assert.Equal(t, rr.interval, int64(interval))
	assert.Equal(t, rr.readStopFlagClientId, int64(readStopFlag))
	assert.Equal(t, rr.readerQueueSize, 8)
	assert.Equal(t, len(rr.proxyIdList), 2)
	assert.Equal(t, rr.proxyIdList[0], int64(1))
	assert.Equal(t, rr.proxyIdList[1], int64(2))
	r.Close()
}

func TestPulsarClient(t *testing.T) {
	t.Skip("skip pulsar client")
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	assert.Nil(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	go startWriteTimeSync(1, timeSyncTopic, client, 2*time.Second, t)
	go startWriteTimeSync(2, timeSyncTopic, client, 2*time.Second, t)
	timeSyncChan := make(chan pulsar.ConsumerMessage)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       timeSyncTopic,
		SubscriptionName:            timeSyncSubName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              timeSyncChan,
	})
	assert.Nil(t, err)

	for {
		select {
		case cm := <-timeSyncChan:
			msg := cm.Message
			var tsm pb.TimeSyncMsg
			if err := proto.Unmarshal(msg.Payload(), &tsm); err != nil {
				log.Fatal(err)
			}
			consumer.AckID(msg.ID())
			log.Printf("read time stamp, id = %d, time stamp = %d\n", tsm.Peer_Id, tsm.Timestamp)
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
}

func TestReaderTimesync(t *testing.T) {
	r, err := NewReaderTimeSync(timeSyncTopic,
		timeSyncSubName,
		[]string{readerTopic1, readerTopic2, readerTopic3, readerTopic4},
		readerSubName,
		[]int64{2, 1},
		readStopFlag,
		WithPulsarAddress("pulsar://localhost:6650"),
		WithInterval(interval),
		WithReaderQueueSize(1024),
	)
	assert.Nil(t, err)

	rr := r.(*ReaderTimeSyncCfg)
	pt1, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic})
	assert.Nil(t, err)

	pt2, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic})
	assert.Nil(t, err)

	pr1, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic1})
	assert.Nil(t, err)

	pr2, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic2})
	assert.Nil(t, err)

	pr3, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic3})
	assert.Nil(t, err)

	pr4, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic4})
	assert.Nil(t, err)

	go startProxy(pt1, 1, pr1, 1, pr2, 2, 2*time.Second, t)
	go startProxy(pt2, 2, pr3, 3, pr4, 4, 2*time.Second, t)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	r.Start()

	var tsm1, tsm2 TimeSyncMsg
	var totalRecordes int64 = 0
	for {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			tsm1.NumRecorders = 0
			break
		case tsm1 = <-r.TimeSync():

		}
		if tsm1.NumRecorders > 0 {
			log.Printf("timestamp %d, num records = %d", getMillisecond(tsm1.Timestamp), tsm1.NumRecorders)
			totalRecordes += tsm1.NumRecorders
			for i := int64(0); i < tsm1.NumRecorders; i++ {
				im := <-r.ManipulationReqMsg()
				//log.Printf("%d - %d", getMillisecond(im.Timestamp), getMillisecond(tsm2.Timestamp))
				if im.Timestamp < tsm2.Timestamp {
					t.Fatalf("time sync error , im.Timestamp = %d, tsm2.Timestamp = %d", im.Timestamp, tsm2.Timestamp)
				}
			}
			tsm2 = tsm1
		}

	}
	log.Printf("total recordes = %d", totalRecordes)
	if totalRecordes != 800 {
		t.Fatalf("total records should be 800")
	}
	r.Close()
	pt1.Close()
	pt2.Close()
	pr1.Close()
	pr2.Close()
	pr3.Close()
	pr4.Close()
}

func TestReaderTimesync2(t *testing.T) {
	client, _ := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	pt1, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic2})
	pt2, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic2})
	pr1, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic12})
	pr2, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic22})
	pr3, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic32})
	pr4, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic42})

	go startProxy(pt1, 1, pr1, 1, pr2, 2, 2*time.Second, t)
	go startProxy(pt2, 2, pr3, 3, pr4, 4, 2*time.Second, t)

	r1, _ := NewReaderTimeSync(timeSyncTopic2,
		timeSyncSubName1,
		[]string{readerTopic12, readerTopic22, readerTopic32, readerTopic42},
		readerSubName1,
		[]int64{2, 1},
		readStopFlag1,
		WithPulsarAddress("pulsar://localhost:6650"),
		WithInterval(interval),
		WithReaderQueueSize(1024),
	)

	r2, _ := NewReaderTimeSync(timeSyncTopic2,
		timeSyncSubName2,
		[]string{readerTopic12, readerTopic22, readerTopic32, readerTopic42},
		readerSubName2,
		[]int64{2, 1},
		readStopFlag2,
		WithPulsarAddress("pulsar://localhost:6650"),
		WithInterval(interval),
		WithReaderQueueSize(1024),
	)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	rt := []ReaderTimeSync{r1, r2}
	var wg sync.WaitGroup
	for _, r := range rt {
		r := r
		_ = r.Start()
		wg.Add(1)
		go func() {
			var tsm1, tsm2 TimeSyncMsg
			var totalRecordes int64 = 0
			work := false
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					break
				}
				select {
				case tsm1 = <-r.TimeSync():
					work = true
				default:
					work = false
				}
				if work {
					if tsm1.NumRecorders > 0 {
						//log.Printf("timestamp %d, num records = %d", getMillisecond(tsm1.Timestamp), tsm1.NumRecorders)
						totalRecordes += tsm1.NumRecorders
						for i := int64(0); i < tsm1.NumRecorders; i++ {
							im := <-r.ManipulationReqMsg()
							//log.Printf("%d - %d", getMillisecond(im.Timestamp), getMillisecond(tsm2.Timestamp))
							assert.GreaterOrEqual(t, im.Timestamp, tsm2.Timestamp)
						}
						tsm2 = tsm1
					}
				}
			}
			log.Printf("total recordes = %d", totalRecordes)
			assert.Equal(t, totalRecordes, int64(800))
		}()
	}
	wg.Wait()
	r1.Close()
	r2.Close()
	pt1.Close()
	pt2.Close()
	pr1.Close()
	pr2.Close()
	pr3.Close()
	pr4.Close()
}

func TestReaderTimesync3(t *testing.T) {
	client, _ := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	pt, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic3})
	pr1, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic13})
	pr2, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic23})
	pr3, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic33})
	pr4, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic43})
	defer func() {
		pr1.Close()
		pr2.Close()
		pr3.Close()
		pr4.Close()
		pt.Close()
		client.Close()
	}()
	go func() {
		total := 2 * 1000 / 10
		ticker := time.Tick(10 * time.Millisecond)
		var timestamp uint64 = 0
		prlist := []pulsar.Producer{pr1, pr2, pr3, pr4}
		for i := 1; i <= total; i++ {
			<-ticker
			timestamp += 10
			for idx, pr := range prlist {
				msg := pb.ManipulationReqMsg{ProxyId: int64(idx + 1), Timestamp: toTimestamp(timestamp)}
				mb, err := proto.Marshal(&msg)
				assert.Nil(t, err)
				if _, err := pr.Send(context.Background(), &pulsar.ProducerMessage{Payload: mb}); err != nil {
					t.Fatal(err)
				}
			}
			if i%20 == 0 {
				tm := pb.TimeSyncMsg{Peer_Id: 1, Timestamp: toTimestamp(timestamp)}
				tb, err := proto.Marshal(&tm)
				assert.Nil(t, err)
				if _, err := pt.Send(context.Background(), &pulsar.ProducerMessage{Payload: tb}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}()

	r, err := NewReaderTimeSync(timeSyncTopic3,
		timeSyncSubName3,
		[]string{readerTopic13, readerTopic23, readerTopic33, readerTopic43},
		readerSubName3,
		[]int64{1},
		readStopFlag3,
		WithPulsarAddress("pulsar://localhost:6650"),
		WithInterval(interval),
		WithReaderQueueSize(1024))
	assert.Nil(t, err)
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	var tsm1, tsm2 TimeSyncMsg
	var totalRecords int64 = 0
	for {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			tsm1.NumRecorders = 0
			break
		case tsm1 = <-r.TimeSync():

		}
		if tsm1.NumRecorders > 0 {
			totalRecords += tsm1.NumRecorders
			for i := int64(0); i < tsm1.NumRecorders; i++ {
				im := <-r.ManipulationReqMsg()
				assert.GreaterOrEqual(t, im.Timestamp, tsm2.Timestamp)
			}
			tsm2 = tsm1
		}
	}
	log.Printf("total records = %d", totalRecords)
	assert.Equal(t, totalRecords, int64(800))
}

func getMillisecond(ts uint64) uint64 {
	return ts >> 18
}

func toTimestamp(ts uint64) uint64 {
	return ts << 18
}

func startWriteTimeSync(id int64, topic string, client pulsar.Client, duration time.Duration, t *testing.T) {
	p, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	ticker := time.Tick(interval * time.Millisecond)
	numSteps := int(duration / (interval * time.Millisecond))
	var tm uint64 = 0
	for i := 0; i < numSteps; i++ {
		<-ticker
		tm += interval
		tsm := pb.TimeSyncMsg{Timestamp: toTimestamp(tm), Peer_Id: id}
		tb, _ := proto.Marshal(&tsm)
		if _, err := p.Send(context.Background(), &pulsar.ProducerMessage{Payload: tb}); err != nil {
			t.Fatalf("send failed tsm id=%d, timestamp=%d, err=%v", tsm.Peer_Id, tsm.Timestamp, err)
		} else {
			//log.Printf("send tsm id=%d, timestamp=%d", tsm.Peer_Id, tsm.Timestamp)
		}
	}
}

func startProxy(pt pulsar.Producer, ptid int64, pr1 pulsar.Producer, prid1 int64, pr2 pulsar.Producer, prid2 int64, duration time.Duration, t *testing.T) {
	total := int(duration / (10 * time.Millisecond))
	ticker := time.Tick(10 * time.Millisecond)
	var timestamp uint64 = 0
	for i := 1; i <= total; i++ {
		<-ticker
		timestamp += 10
		msg := pb.ManipulationReqMsg{ProxyId: int64(prid1), Timestamp: toTimestamp(timestamp)}
		mb, err := proto.Marshal(&msg)
		if err != nil {
			t.Fatalf("marshal error %v", err)
		}
		if _, err := pr1.Send(context.Background(), &pulsar.ProducerMessage{Payload: mb}); err != nil {
			t.Fatalf("send msg error %v", err)
		}

		msg.ProxyId = prid2
		mb, err = proto.Marshal(&msg)
		if err != nil {
			t.Fatalf("marshal error %v", err)
		}
		if _, err := pr2.Send(context.Background(), &pulsar.ProducerMessage{Payload: mb}); err != nil {
			t.Fatalf("send msg error %v", err)
		}

		//log.Printf("send msg id = [ %d %d ], timestamp = %d", prid1, prid2, timestamp)

		if i%20 == 0 {
			tm := pb.TimeSyncMsg{Peer_Id: ptid, Timestamp: toTimestamp(timestamp)}
			tb, err := proto.Marshal(&tm)
			if err != nil {
				t.Fatalf("marshal error %v", err)
			}
			if _, err := pt.Send(context.Background(), &pulsar.ProducerMessage{Payload: tb}); err != nil {
				t.Fatalf("send msg error %v", err)
			}
			//log.Printf("send timestamp id = %d, timestamp = %d", ptid, timestamp)
		}
	}
}
