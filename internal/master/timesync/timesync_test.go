package timesync

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func getTtMsg(msgType internalPb.MsgType, peerID UniqueID, timeStamp uint64) ms.TsMsg {
	baseMsg := ms.BaseMsg{
		HashValues: []int32{int32(peerID)},
	}
	timeTickResult := internalPb.TimeTickMsg{
		MsgType:   internalPb.MsgType_kTimeTick,
		PeerID:    peerID,
		Timestamp: timeStamp,
	}
	timeTickMsg := &ms.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}

	return timeTickMsg
}

func initPulsarStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string) (*ms.MsgStream, *ms.MsgStream) {

	// set input stream
	inputStream := ms.NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	var input ms.MsgStream = inputStream

	// set output stream
	outputStream := ms.NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := ms.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output ms.MsgStream = outputStream

	return &input, &output
}

func getMsgPack(ttmsgs [][2]int) *ms.MsgPack {
	msgPack := ms.MsgPack{}
	for _, vi := range ttmsgs {
		msgPack.Msgs = append(msgPack.Msgs, getTtMsg(internalPb.MsgType_kTimeTick, UniqueID(vi[0]), Timestamp(vi[1])))
	}
	return &msgPack
}

func getEmptyMsgPack() *ms.MsgPack {
	msgPack := ms.MsgPack{}
	return &msgPack
}

func producer(channels []string, ttmsgs [][2]int) (*ms.MsgStream, *ms.MsgStream) {
	pulsarAddress := "pulsar://localhost:6650"
	consumerSubName := "subTimetick"
	producerChannels := channels
	consumerChannels := channels

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)

	msgPackAddr := getMsgPack(ttmsgs)
	(*inputStream).Produce(msgPackAddr)
	return inputStream, outputStream
}

func TestTt_NewSoftTtBarrier(t *testing.T) {
	channels := []string{"NewSoftTtBarrier"}
	ttmsgs := [][2]int{
		{1, 10},
		{2, 20},
		{3, 30},
		{4, 40},
		{1, 30},
		{2, 30},
	}

	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	minTtInterval := Timestamp(10)

	validPeerIds := []UniqueID{1, 2, 3}

	sttbarrier := NewSoftTimeTickBarrier(context.TODO(), ttStream, validPeerIds, minTtInterval)
	assert.NotNil(t, sttbarrier)
	sttbarrier.Close()

	validPeerIds2 := []UniqueID{1, 1, 1}
	sttbarrier = NewSoftTimeTickBarrier(context.TODO(), ttStream, validPeerIds2, minTtInterval)
	assert.NotNil(t, sttbarrier)
	sttbarrier.Close()

	// invalid peerIds
	invalidPeerIds1 := make([]UniqueID, 0, 3)
	sttbarrier = NewSoftTimeTickBarrier(context.TODO(), ttStream, invalidPeerIds1, minTtInterval)
	assert.Nil(t, sttbarrier)

	invalidPeerIds2 := []UniqueID{}
	sttbarrier = NewSoftTimeTickBarrier(context.TODO(), ttStream, invalidPeerIds2, minTtInterval)
	assert.Nil(t, sttbarrier)
}

func TestTt_NewHardTtBarrier(t *testing.T) {
	channels := []string{"NewHardTtBarrier"}
	ttmsgs := [][2]int{
		{1, 10},
		{2, 20},
		{3, 30},
		{4, 40},
		{1, 30},
		{2, 30},
	}
	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	validPeerIds := []UniqueID{1, 2, 3}

	sttbarrier := NewHardTimeTickBarrier(context.TODO(), ttStream, validPeerIds)
	assert.NotNil(t, sttbarrier)
	sttbarrier.Close()

	validPeerIds2 := []UniqueID{1, 1, 1}
	sttbarrier = NewHardTimeTickBarrier(context.TODO(), ttStream, validPeerIds2)
	assert.NotNil(t, sttbarrier)
	sttbarrier.Close()

	// invalid peerIds
	invalidPeerIds1 := make([]UniqueID, 0, 3)
	sttbarrier = NewHardTimeTickBarrier(context.TODO(), ttStream, invalidPeerIds1)
	assert.Nil(t, sttbarrier)

	invalidPeerIds2 := []UniqueID{}
	sttbarrier = NewHardTimeTickBarrier(context.TODO(), ttStream, invalidPeerIds2)
	assert.Nil(t, sttbarrier)
}

func TestTt_SoftTtBarrierStart(t *testing.T) {
	channels := []string{"SoftTtBarrierStart"}

	ttmsgs := [][2]int{
		{1, 10},
		{2, 20},
		{3, 30},
		{4, 40},
		{1, 30},
		{2, 30},
	}
	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	minTtInterval := Timestamp(10)
	peerIds := []UniqueID{1, 2, 3}
	sttbarrier := NewSoftTimeTickBarrier(context.TODO(), ttStream, peerIds, minTtInterval)
	require.NotNil(t, sttbarrier)

	sttbarrier.Start()
	defer sttbarrier.Close()

	// Make sure all msgs in outputStream is consumed
	time.Sleep(100 * time.Millisecond)

	ts, err := sttbarrier.GetTimeTick()
	assert.Nil(t, err)
	assert.Equal(t, Timestamp(30), ts)
}

func TestTt_SoftTtBarrierGetTimeTickClose(t *testing.T) {
	channels := []string{"SoftTtBarrierGetTimeTickClose"}
	ttmsgs := [][2]int{
		{1, 10},
		{2, 20},
		{3, 30},
		{4, 40},
		{1, 30},
		{2, 30},
	}
	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	minTtInterval := Timestamp(10)
	validPeerIds := []UniqueID{1, 2, 3}

	sttbarrier := NewSoftTimeTickBarrier(context.TODO(), ttStream, validPeerIds, minTtInterval)
	require.NotNil(t, sttbarrier)

	sttbarrier.Start()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		sttbarrier.Close()
	}()
	wg.Wait()

	ts, err := sttbarrier.GetTimeTick()
	assert.NotNil(t, err)
	assert.Equal(t, Timestamp(0), ts)

	// Receive empty msgPacks
	channels01 := []string{"GetTimeTick01"}
	ttmsgs01 := [][2]int{}
	inStream01, ttStream01 := producer(channels01, ttmsgs01)
	defer func() {
		(*inStream01).Close()
		(*ttStream01).Close()
	}()

	minTtInterval = Timestamp(10)
	validPeerIds = []UniqueID{1, 2, 3}

	sttbarrier01 := NewSoftTimeTickBarrier(context.TODO(), ttStream01, validPeerIds, minTtInterval)
	require.NotNil(t, sttbarrier01)
	sttbarrier01.Start()

	var wg1 sync.WaitGroup
	wg1.Add(1)

	go func() {
		defer wg1.Done()
		sttbarrier01.Close()
	}()

	wg1.Wait()
	ts, err = sttbarrier01.GetTimeTick()
	assert.NotNil(t, err)
	assert.Equal(t, Timestamp(0), ts)
}

func TestTt_SoftTtBarrierGetTimeTickCancel(t *testing.T) {
	channels := []string{"SoftTtBarrierGetTimeTickCancel"}
	ttmsgs := [][2]int{
		{1, 10},
		{2, 20},
		{3, 30},
		{4, 40},
		{1, 30},
		{2, 30},
	}
	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	minTtInterval := Timestamp(10)
	validPeerIds := []UniqueID{1, 2, 3}

	ctx, cancel := context.WithCancel(context.Background())
	sttbarrier := NewSoftTimeTickBarrier(ctx, ttStream, validPeerIds, minTtInterval)
	require.NotNil(t, sttbarrier)

	sttbarrier.Start()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cancel()
		time.Sleep(100 * time.Millisecond)
	}()

	wg.Wait()

	ts, err := sttbarrier.GetTimeTick()
	assert.NotNil(t, err)
	assert.Equal(t, Timestamp(0), ts)
	log.Println(err)
}

func TestTt_HardTtBarrierStart(t *testing.T) {
	channels := []string{"HardTtBarrierStart"}

	ttmsgs := [][2]int{
		{1, 10},
		{2, 10},
		{3, 10},
	}

	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	peerIds := []UniqueID{1, 2, 3}
	sttbarrier := NewHardTimeTickBarrier(context.TODO(), ttStream, peerIds)
	require.NotNil(t, sttbarrier)

	sttbarrier.Start()
	defer sttbarrier.Close()

	// Make sure all msgs in outputStream is consumed
	time.Sleep(100 * time.Millisecond)

	ts, err := sttbarrier.GetTimeTick()
	assert.Nil(t, err)
	assert.Equal(t, Timestamp(10), ts)
}

func TestTt_HardTtBarrierGetTimeTick(t *testing.T) {

	channels := []string{"HardTtBarrierGetTimeTick"}

	ttmsgs := [][2]int{
		{1, 10},
		{1, 20},
		{1, 30},
		{2, 10},
		{2, 20},
		{3, 10},
		{3, 20},
	}

	inStream, ttStream := producer(channels, ttmsgs)
	defer func() {
		(*inStream).Close()
		(*ttStream).Close()
	}()

	peerIds := []UniqueID{1, 2, 3}
	sttbarrier := NewHardTimeTickBarrier(context.TODO(), ttStream, peerIds)
	require.NotNil(t, sttbarrier)

	sttbarrier.Start()
	defer sttbarrier.Close()

	// Make sure all msgs in outputStream is consumed
	time.Sleep(100 * time.Millisecond)

	ts, err := sttbarrier.GetTimeTick()
	assert.Nil(t, err)
	assert.Equal(t, Timestamp(10), ts)

	ts, err = sttbarrier.GetTimeTick()
	assert.Nil(t, err)
	assert.Equal(t, Timestamp(20), ts)

	// ---------------------stuck--------------------------
	channelsStuck := []string{"HardTtBarrierGetTimeTickStuck"}

	ttmsgsStuck := [][2]int{
		{1, 10},
		{2, 10},
	}

	inStreamStuck, ttStreamStuck := producer(channelsStuck, ttmsgsStuck)
	defer func() {
		(*inStreamStuck).Close()
		(*ttStreamStuck).Close()
	}()

	peerIdsStuck := []UniqueID{1, 2, 3}
	sttbarrierStuck := NewHardTimeTickBarrier(context.TODO(), ttStreamStuck, peerIdsStuck)
	require.NotNil(t, sttbarrierStuck)

	sttbarrierStuck.Start()
	go func() {
		time.Sleep(1 * time.Second)
		sttbarrierStuck.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	// This will stuck
	ts, err = sttbarrierStuck.GetTimeTick()
	assert.NotNil(t, err)
	assert.Equal(t, Timestamp(0), ts)

	// ---------------------context cancel------------------------
	channelsCancel := []string{"HardTtBarrierGetTimeTickCancel"}

	ttmsgsCancel := [][2]int{
		{1, 10},
		{2, 10},
	}

	inStreamCancel, ttStreamCancel := producer(channelsCancel, ttmsgsCancel)
	defer func() {
		(*inStreamCancel).Close()
		(*ttStreamCancel).Close()
	}()

	peerIdsCancel := []UniqueID{1, 2, 3}

	ctx, cancel := context.WithCancel(context.Background())
	sttbarrierCancel := NewHardTimeTickBarrier(ctx, ttStreamCancel, peerIdsCancel)
	require.NotNil(t, sttbarrierCancel)

	sttbarrierCancel.Start()
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()

	time.Sleep(100 * time.Millisecond)

	// This will stuck
	ts, err = sttbarrierCancel.GetTimeTick()
	assert.NotNil(t, err)
	assert.Equal(t, Timestamp(0), ts)
}
