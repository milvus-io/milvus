package timesync

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/golang/protobuf/proto"
)

const (
	pulsarAddr             = "pulsar://localhost:6650"
	timeSyncTopic          = "timesync"
	timeSyncTopic2         = "timesync2"
	timeSyncTopic3         = "timesync3"
	timeSyncSubName        = "timesync-g"
	timeSyncSubName1       = "timesync-g1"
	timeSyncSubName2       = "timesync-g2"
	timeSyncSubName3       = "timesync-g3"
	readerTopic1           = "reader1"
	readerTopic12          = "reader12"
	readerTopic13          = "reader13"
	readerTopic2           = "reader2"
	readerTopic22          = "reader22"
	readerTopic23          = "reader23"
	readerTopic3           = "reader3"
	readerTopic32          = "reader32"
	readerTopic33          = "reader33"
	readerTopic4           = "reader4"
	readerTopic42          = "reader42"
	readerTopic43          = "reader43"
	readerSubName          = "reader-g"
	readerSubName1         = "reader-g1"
	readerSubName2         = "reader-g2"
	readerSubName3         = "reader-g3"
	interval               = 200
	readStopFlag     int64 = -1
	readStopFlag1    int64 = -1
	readStopFlag2    int64 = -2
	readStopFlag3    int64 = -3
)

func TestAlignTimeSync(t *testing.T) {
	r := &TimeSyncCfg{
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
	if len(r.proxyIdList) != 3 {
		t.Fatalf("proxyIdList should be : 1 2 3")
	}
	for i := 0; i < len(r.proxyIdList); i++ {
		if r.proxyIdList[i] != ts[i].Peer_Id {
			t.Fatalf("Align falied")
		}
	}

}

func TestAlignTimeSync2(t *testing.T) {
	r := &TimeSyncCfg{
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
	if len(r.proxyIdList) != 3 {
		t.Fatalf("proxyIdList should be : 1 2 3")
	}
	if len(ts) != 1 || ts[0].Peer_Id != 2 {
		t.Fatalf("align failed")
	}

}

func TestAlignTimeSync3(t *testing.T) {
	r := &TimeSyncCfg{
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
	if len(r.proxyIdList) != 3 {
		t.Fatalf("proxyIdList should be : 1 2 3")
	}
	for i := 0; i < len(r.proxyIdList); i++ {
		if r.proxyIdList[i] != ts[i].Peer_Id {
			t.Fatalf("Align falied")
		}
	}
}

func TestAlignTimeSync4(t *testing.T) {
	r := &TimeSyncCfg{
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
	if len(r.proxyIdList) != 1 {
		t.Fatalf("proxyIdList should be : 1")
	}
	if len(ts) != 1 {
		t.Fatalf("aligned failed")
	}
	if getMillisecond(ts[0].Timestamp) != 35 {
		t.Fatalf("aligned failed")
	}
}

func TestAlignTimeSync5(t *testing.T) {
	r := &TimeSyncCfg{
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
	if len(ts) != 0 {
		t.Fatalf("aligned failed")
	}
}

func TestNewTimeSync(t *testing.T) {
	r, err := NewTimeSync(context.Background(),
		timeSyncTopic,
		timeSyncSubName,
		[]string{readerTopic1, readerTopic2, readerTopic3, readerTopic4},
		readerSubName,
		[]int64{2, 1},
		interval,
		WithReaderQueueSize(8),
	)
	if err != nil {
		t.Fatal(err)
	}
	rr := r.(*TimeSyncCfg)
	if rr.pulsarClient == nil {
		t.Fatalf("create pulsar client failed")
	}
	if rr.timeSyncConsumer == nil {
		t.Fatalf("create time sync consumer failed")
	}
	if rr.readerConsumer == nil {
		t.Fatalf("create reader consumer failed")
	}
	if len(rr.readerProducer) != 4 {
		t.Fatalf("create reader producer failed")
	}
	if rr.interval != interval {
		t.Fatalf("interval shoudl be %d", interval)
	}
	if rr.readStopFlagClientId != readStopFlag {
		t.Fatalf("raed stop flag client id should be %d", rr.readStopFlagClientId)
	}
	if rr.readerQueueSize != 8 {
		t.Fatalf("set read queue size failed")
	}
	if len(rr.proxyIdList) != 2 {
		t.Fatalf("set proxy id failed")
	}
	if rr.proxyIdList[0] != 1 || rr.proxyIdList[1] != 2 {
		t.Fatalf("set proxy id failed")
	}
	r.Close()
}

func TestPulsarClient(t *testing.T) {
	t.Skip("skip pulsar client")
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddr})
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		log.Fatal(err)
	}
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

func TestTimeSync(t *testing.T) {
	r, err := NewTimeSync(context.Background(),
		timeSyncTopic,
		timeSyncSubName,
		[]string{readerTopic1, readerTopic2, readerTopic3, readerTopic4},
		readerSubName,
		[]int64{2, 1},
		interval,
		WithReaderQueueSize(1024),
	)
	if err != nil {
		t.Fatal(err)
	}
	rr := r.(*TimeSyncCfg)
	pt1, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic})
	if err != nil {
		t.Fatalf("create time sync producer 1 error %v", err)
	}

	pt2, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic})
	if err != nil {
		t.Fatalf("create time sync producer 2 error %v", err)
	}

	pr1, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic1})
	if err != nil {
		t.Fatalf("create reader 1 error %v", err)
	}

	pr2, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic2})
	if err != nil {
		t.Fatalf("create reader 2 error %v", err)
	}

	pr3, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic3})
	if err != nil {
		t.Fatalf("create reader 3 error %v", err)
	}

	pr4, err := rr.pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic4})
	if err != nil {
		t.Fatalf("create reader 4 error %v", err)
	}

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
				im := <-r.InsertOrDelete()
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

func TestTimeSync2(t *testing.T) {
	client, _ := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddr})
	pt1, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic2})
	pt2, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: timeSyncTopic2})
	pr1, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic12})
	pr2, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic22})
	pr3, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic32})
	pr4, _ := client.CreateProducer(pulsar.ProducerOptions{Topic: readerTopic42})

	go startProxy(pt1, 1, pr1, 1, pr2, 2, 2*time.Second, t)
	go startProxy(pt2, 2, pr3, 3, pr4, 4, 2*time.Second, t)

	r1, _ := NewTimeSync(context.Background(),
		timeSyncTopic2,
		timeSyncSubName1,
		[]string{readerTopic12, readerTopic22, readerTopic32, readerTopic42},
		readerSubName1,
		[]int64{2, 1},
		interval,
		WithReaderQueueSize(1024),
	)

	r2, _ := NewTimeSync(context.Background(),
		timeSyncTopic2,
		timeSyncSubName2,
		[]string{readerTopic12, readerTopic22, readerTopic32, readerTopic42},
		readerSubName2,
		[]int64{2, 1},
		interval,
		WithReaderQueueSize(1024),
	)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	rt := []TimeSync{r1, r2}
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
							im := <-r.InsertOrDelete()
							//log.Printf("%d - %d", getMillisecond(im.Timestamp), getMillisecond(tsm2.Timestamp))
							if im.Timestamp < tsm2.Timestamp {
								t.Fatalf("time sync error , im.Timestamp = %d, tsm2.Timestamp = %d", getMillisecond(im.Timestamp), getMillisecond(tsm2.Timestamp))
							}
						}
						tsm2 = tsm1
					}
				}
			}
			log.Printf("total recordes = %d", totalRecordes)
			if totalRecordes != 800 {
				t.Fatalf("total records should be 800")
			}
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

func TestTimeSync3(t *testing.T) {
	client, _ := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddr})
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
				msg := pb.InsertOrDeleteMsg{ClientId: int64(idx + 1), Timestamp: toTimestamp(timestamp)}
				mb, err := proto.Marshal(&msg)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pr.Send(context.Background(), &pulsar.ProducerMessage{Payload: mb}); err != nil {
					t.Fatal(err)
				}
			}
			if i%20 == 0 {
				tm := pb.TimeSyncMsg{Peer_Id: 1, Timestamp: toTimestamp(timestamp)}
				tb, err := proto.Marshal(&tm)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pt.Send(context.Background(), &pulsar.ProducerMessage{Payload: tb}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}()

	r, err := NewTimeSync(context.Background(),
		timeSyncTopic3,
		timeSyncSubName3,
		[]string{readerTopic13, readerTopic23, readerTopic33, readerTopic43},
		readerSubName3,
		[]int64{1},
		interval,
		WithReaderQueueSize(1024))
	if err != nil {
		t.Fatal(err)
	}
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
				im := <-r.InsertOrDelete()
				if im.Timestamp < tsm2.Timestamp {
					t.Fatalf("time sync error , im.Timestamp = %d, tsm2.Timestamp = %d", im.Timestamp, tsm2.Timestamp)
				}
			}
			tsm2 = tsm1
		}
	}
	log.Printf("total records = %d", totalRecords)
	if totalRecords != 800 {
		t.Fatalf("total records should be 800")
	}

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
		msg := pb.InsertOrDeleteMsg{ClientId: prid1, Timestamp: toTimestamp(timestamp)}
		mb, err := proto.Marshal(&msg)
		if err != nil {
			t.Fatalf("marshal error %v", err)
		}
		if _, err := pr1.Send(context.Background(), &pulsar.ProducerMessage{Payload: mb}); err != nil {
			t.Fatalf("send msg error %v", err)
		}

		msg.ClientId = prid2
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
