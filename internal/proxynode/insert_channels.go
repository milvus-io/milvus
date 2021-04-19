package proxynode

import (
	"context"
	"log"
	"reflect"
	"sort"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"

	"github.com/zilliztech/milvus-distributed/internal/errors"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

func SliceContain(s interface{}, item interface{}) bool {
	ss := reflect.ValueOf(s)
	if ss.Kind() != reflect.Slice {
		panic("SliceContain expect a slice")
	}

	for i := 0; i < ss.Len(); i++ {
		if ss.Index(i).Interface() == item {
			return true
		}
	}

	return false
}

func SliceSetEqual(s1 interface{}, s2 interface{}) bool {
	ss1 := reflect.ValueOf(s1)
	ss2 := reflect.ValueOf(s2)
	if ss1.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss2.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss1.Len() != ss2.Len() {
		return false
	}
	for i := 0; i < ss1.Len(); i++ {
		if !SliceContain(s2, ss1.Index(i).Interface()) {
			return false
		}
	}
	return true
}

func SortedSliceEqual(s1 interface{}, s2 interface{}) bool {
	ss1 := reflect.ValueOf(s1)
	ss2 := reflect.ValueOf(s2)
	if ss1.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss2.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss1.Len() != ss2.Len() {
		return false
	}
	for i := 0; i < ss1.Len(); i++ {
		if ss2.Index(i).Interface() != ss1.Index(i).Interface() {
			return false
		}
	}
	return true
}

type InsertChannelsMap struct {
	collectionID2InsertChannels map[UniqueID]int      // the value of map is the location of insertChannels & insertMsgStreams
	insertChannels              [][]string            // it's a little confusing to use []string as the key of map
	insertMsgStreams            []msgstream.MsgStream // maybe there's a better way to implement Set, just agilely now
	droppedBitMap               []int                 // 0 -> normal, 1 -> dropped
	usageHistogram              []int                 // message stream can be closed only when the use count is zero
	mtx                         sync.RWMutex
	nodeInstance                *NodeImpl
}

func (m *InsertChannelsMap) createInsertMsgStream(collID UniqueID, channels []string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	_, ok := m.collectionID2InsertChannels[collID]
	if ok {
		return errors.New("impossible and forbidden to create message stream twice")
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i] <= channels[j]
	})
	for loc, existedChannels := range m.insertChannels {
		if m.droppedBitMap[loc] == 0 && SortedSliceEqual(existedChannels, channels) {
			m.collectionID2InsertChannels[collID] = loc
			m.usageHistogram[loc]++
			return nil
		}
	}
	m.insertChannels = append(m.insertChannels, channels)
	m.collectionID2InsertChannels[collID] = len(m.insertChannels) - 1
	stream := pulsarms.NewPulsarMsgStream(context.Background(), Params.MsgStreamInsertBufSize)
	stream.SetPulsarClient(Params.PulsarAddress)
	stream.CreatePulsarProducers(channels)
	repack := func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
		return insertRepackFunc(tsMsgs, hashKeys, m.nodeInstance.segAssigner, true)
	}
	stream.SetRepackFunc(repack)
	stream.Start()
	m.insertMsgStreams = append(m.insertMsgStreams, stream)
	m.droppedBitMap = append(m.droppedBitMap, 0)
	m.usageHistogram = append(m.usageHistogram, 1)

	return nil
}

func (m *InsertChannelsMap) closeInsertMsgStream(collID UniqueID) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	loc, ok := m.collectionID2InsertChannels[collID]
	if !ok {
		return errors.New("cannot find collection with id: " + strconv.Itoa(int(collID)))
	}
	if m.droppedBitMap[loc] != 0 {
		return errors.New("insert message stream already closed")
	}
	if m.usageHistogram[loc] <= 0 {
		return errors.New("insert message stream already closed")
	}

	m.usageHistogram[loc]--
	if m.usageHistogram[loc] <= 0 {
		m.insertMsgStreams[loc].Close()
	}
	log.Print("close insert message stream ...")

	m.droppedBitMap[loc] = 1
	delete(m.collectionID2InsertChannels, collID)

	return nil
}

func (m *InsertChannelsMap) getInsertChannels(collID UniqueID) ([]string, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	loc, ok := m.collectionID2InsertChannels[collID]
	if !ok {
		return nil, errors.New("cannot find collection with id: " + strconv.Itoa(int(collID)))
	}

	if m.droppedBitMap[loc] != 0 {
		return nil, errors.New("insert message stream already closed")
	}
	ret := append([]string(nil), m.insertChannels[loc]...)
	return ret, nil
}

func (m *InsertChannelsMap) getInsertMsgStream(collID UniqueID) (msgstream.MsgStream, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	loc, ok := m.collectionID2InsertChannels[collID]
	if !ok {
		return nil, errors.New("cannot find collection with id: " + strconv.Itoa(int(collID)))
	}

	if m.droppedBitMap[loc] != 0 {
		return nil, errors.New("insert message stream already closed")
	}

	return m.insertMsgStreams[loc], nil
}

func (m *InsertChannelsMap) closeAllMsgStream() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, stream := range m.insertMsgStreams {
		stream.Close()
	}

	m.collectionID2InsertChannels = make(map[UniqueID]int)
	m.insertChannels = make([][]string, 0)
	m.insertMsgStreams = make([]msgstream.MsgStream, 0)
	m.droppedBitMap = make([]int, 0)
	m.usageHistogram = make([]int, 0)
}

func newInsertChannelsMap(node *NodeImpl) *InsertChannelsMap {
	return &InsertChannelsMap{
		collectionID2InsertChannels: make(map[UniqueID]int),
		insertChannels:              make([][]string, 0),
		insertMsgStreams:            make([]msgstream.MsgStream, 0),
		droppedBitMap:               make([]int, 0),
		usageHistogram:              make([]int, 0),
		nodeInstance:                node,
	}
}

var globalInsertChannelsMap *InsertChannelsMap

func initGlobalInsertChannelsMap(node *NodeImpl) {
	globalInsertChannelsMap = newInsertChannelsMap(node)
}
