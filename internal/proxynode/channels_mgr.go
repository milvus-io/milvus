package proxynode

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/msgstream"
)

type vChan = string
type pChan = string

type channelsMgr interface {
	getChannels(collectionID UniqueID) ([]pChan, error)
	getVChannels(collectionID UniqueID) ([]vChan, error)
	createDQLMsgStream(collectionID UniqueID) error
	getDQLMsgStream(collectionID UniqueID) error
	removeDQLMsgStream(collectionID UniqueID) error
	createDMLMsgStream(collectionID UniqueID) error
	getDMLStream(collectionID UniqueID) (msgstream.MsgStream, error)
	removeDMLStream(collectionID UniqueID) error
	removeAllDMLStream() error
}

type (
	uniqueIntGenerator interface {
		get() int
	}
	naiveUniqueIntGenerator struct {
		now int
		mtx sync.Mutex
	}
)

func (generator *naiveUniqueIntGenerator) get() int {
	generator.mtx.Lock()
	defer func() {
		generator.now++
		generator.mtx.Unlock()
	}()
	return generator.now
}

func newNaiveUniqueIntGenerator() *naiveUniqueIntGenerator {
	return &naiveUniqueIntGenerator{
		now: 0,
	}
}

var uniqueIntGeneratorIns uniqueIntGenerator
var getUniqueIntGeneratorInsOnce sync.Once

func getUniqueIntGeneratorIns() uniqueIntGenerator {
	getUniqueIntGeneratorInsOnce.Do(func() {
		uniqueIntGeneratorIns = newNaiveUniqueIntGenerator()
	})
	return uniqueIntGeneratorIns
}

type masterService interface {
	GetChannels(collectionID UniqueID) (map[vChan]pChan, error)
}

type mockMaster struct {
	collectionID2Channels map[UniqueID]map[vChan]pChan
}

func newMockMaster() *mockMaster {
	return &mockMaster{
		collectionID2Channels: make(map[UniqueID]map[vChan]pChan),
	}
}

func genUniqueStr() string {
	l := rand.Uint64()%100 + 1
	b := make([]byte, l)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return fmt.Sprintf("%X", b)
}

func (m *mockMaster) GetChannels(collectionID UniqueID) (map[vChan]pChan, error) {
	channels, ok := m.collectionID2Channels[collectionID]
	if ok {
		return channels, nil
	}

	channels = make(map[vChan]pChan)
	l := rand.Uint64()%10 + 1
	for i := 0; uint64(i) < l; i++ {
		channels[genUniqueStr()] = genUniqueStr()
	}

	m.collectionID2Channels[collectionID] = channels
	return channels, nil
}

type channelsMgrImpl struct {
	collectionID2VIDs map[UniqueID][]int // id are sorted
	collMtx           sync.RWMutex

	id2vchans    map[int][]vChan
	id2vchansMtx sync.RWMutex

	id2DMLStream                 map[int]msgstream.MsgStream
	id2UsageHistogramOfDMLStream map[int]int
	dmlStreamMtx                 sync.RWMutex

	vchans2pchans    map[vChan]pChan
	vchans2pchansMtx sync.RWMutex

	master masterService

	msgStreamFactory msgstream.Factory
}

func getAllKeys(m map[vChan]pChan) []vChan {
	keys := make([]vChan, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func getAllValues(m map[vChan]pChan) []pChan {
	values := make([]pChan, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}

func (mgr *channelsMgrImpl) getLatestVID(collectionID UniqueID) (int, error) {
	mgr.collMtx.RLock()
	defer mgr.collMtx.RUnlock()

	ids, ok := mgr.collectionID2VIDs[collectionID]
	if !ok || len(ids) <= 0 {
		return 0, fmt.Errorf("collection %d not found", collectionID)
	}

	return ids[len(ids)-1], nil
}

func (mgr *channelsMgrImpl) getAllVIDs(collectionID UniqueID) ([]int, error) {
	mgr.collMtx.RLock()
	defer mgr.collMtx.RUnlock()

	ids, ok := mgr.collectionID2VIDs[collectionID]
	if !ok {
		return nil, fmt.Errorf("collection %d not found", collectionID)
	}

	return ids, nil
}

func (mgr *channelsMgrImpl) getVChansByVID(vid int) ([]vChan, error) {
	mgr.id2vchansMtx.RLock()
	defer mgr.id2vchansMtx.RUnlock()

	vchans, ok := mgr.id2vchans[vid]
	if !ok {
		return nil, fmt.Errorf("vid %d not found", vid)
	}

	return vchans, nil
}

func (mgr *channelsMgrImpl) getPChansByVChans(vchans []vChan) ([]pChan, error) {
	mgr.vchans2pchansMtx.RLock()
	defer mgr.vchans2pchansMtx.RUnlock()

	pchans := make([]pChan, 0)
	for _, vchan := range vchans {
		pchan, ok := mgr.vchans2pchans[vchan]
		if !ok {
			return nil, fmt.Errorf("vchan %v not found", vchan)
		}
		pchans = append(pchans, pchan)
	}

	return pchans, nil
}

func (mgr *channelsMgrImpl) updateVChans(vid int, vchans []vChan) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2vchans[vid] = vchans
}

func (mgr *channelsMgrImpl) deleteVChansByVID(vid int) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	delete(mgr.id2vchans, vid)
}

func (mgr *channelsMgrImpl) deleteVChansByVIDs(vids []int) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	for _, vid := range vids {
		delete(mgr.id2vchans, vid)
	}
}

func (mgr *channelsMgrImpl) deleteDMLStreamByVID(vid int) {
	mgr.dmlStreamMtx.Lock()
	defer mgr.dmlStreamMtx.Unlock()

	delete(mgr.id2DMLStream, vid)
}

func (mgr *channelsMgrImpl) deleteDMLStreamByVIDs(vids []int) {
	mgr.dmlStreamMtx.Lock()
	defer mgr.dmlStreamMtx.Unlock()

	for _, vid := range vids {
		delete(mgr.id2DMLStream, vid)
	}
}

func (mgr *channelsMgrImpl) updateChannels(channels map[vChan]pChan) {
	mgr.vchans2pchansMtx.Lock()
	defer mgr.vchans2pchansMtx.Unlock()

	for vchan, pchan := range channels {
		mgr.vchans2pchans[vchan] = pchan
	}
}

func (mgr *channelsMgrImpl) deleteAllChannels() {
	mgr.vchans2pchansMtx.Lock()
	defer mgr.vchans2pchansMtx.Unlock()

	mgr.vchans2pchans = nil
}

func (mgr *channelsMgrImpl) deleteAllDMLStream() {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2UsageHistogramOfDMLStream = nil
	mgr.id2DMLStream = nil
}

func (mgr *channelsMgrImpl) deleteAllVChans() {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2vchans = nil
}

func (mgr *channelsMgrImpl) deleteAllCollection() {
	mgr.collMtx.Lock()
	defer mgr.collMtx.Unlock()

	mgr.collectionID2VIDs = nil
}

func (mgr *channelsMgrImpl) addDMLStream(vid int, stream msgstream.MsgStream) {
	mgr.dmlStreamMtx.Lock()
	defer mgr.dmlStreamMtx.Unlock()

	mgr.id2DMLStream[vid] = stream
	mgr.id2UsageHistogramOfDMLStream[vid] = 0
}

func (mgr *channelsMgrImpl) updateCollection(collectionID UniqueID, id int) {
	mgr.collMtx.Lock()
	defer mgr.collMtx.Unlock()

	vids, ok := mgr.collectionID2VIDs[collectionID]
	if !ok {
		mgr.collectionID2VIDs[collectionID] = make([]int, 1)
		mgr.collectionID2VIDs[collectionID][0] = id
	} else {
		vids = append(vids, id)
		sort.Slice(vids, func(i, j int) bool {
			return vids[i] < vids[j]
		})
		mgr.collectionID2VIDs[collectionID] = vids
	}
}

func (mgr *channelsMgrImpl) getChannels(collectionID UniqueID) ([]pChan, error) {
	id, err := mgr.getLatestVID(collectionID)
	if err == nil {
		vchans, err := mgr.getVChansByVID(id)
		if err != nil {
			return nil, err
		}

		return mgr.getPChansByVChans(vchans)
	}

	// TODO(dragondriver): return error or update channel information from master?
	return nil, err
}

func (mgr *channelsMgrImpl) getVChannels(collectionID UniqueID) ([]vChan, error) {
	id, err := mgr.getLatestVID(collectionID)
	if err == nil {
		return mgr.getVChansByVID(id)
	}

	// TODO(dragondriver): return error or update channel information from master?
	return nil, err
}

func (mgr *channelsMgrImpl) createDQLMsgStream(collectionID UniqueID) error {
	panic("implement me")
}

func (mgr *channelsMgrImpl) getDQLMsgStream(collectionID UniqueID) error {
	panic("implement me")
}

func (mgr *channelsMgrImpl) removeDQLMsgStream(collectionID UniqueID) error {
	panic("implement me")
}

func (mgr *channelsMgrImpl) createDMLMsgStream(collectionID UniqueID) error {
	channels, err := mgr.master.GetChannels(collectionID)
	if err != nil {
		return err
	}

	mgr.updateChannels(channels)

	id := getUniqueIntGeneratorIns().get()
	vchans := getAllKeys(channels)
	mgr.updateVChans(id, vchans)

	stream, err := mgr.msgStreamFactory.NewMsgStream(context.Background())
	if err != nil {
		return err
	}
	pchans := getAllValues(channels)
	stream.AsProducer(pchans)
	repack := func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
		// TODO(dragondriver): use new repack function later
		return nil, nil
	}
	stream.SetRepackFunc(repack)
	runtime.SetFinalizer(stream, func(stream msgstream.MsgStream) {
		stream.Close()
	})
	mgr.addDMLStream(id, stream)

	mgr.updateCollection(collectionID, id)

	return nil
}

func (mgr *channelsMgrImpl) getDMLStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	mgr.dmlStreamMtx.RLock()
	defer mgr.dmlStreamMtx.RUnlock()

	vid, err := mgr.getLatestVID(collectionID)
	if err != nil {
		return nil, err
	}

	stream, ok := mgr.id2DMLStream[vid]
	if !ok {
		return nil, fmt.Errorf("no dml stream for collection %v", collectionID)
	}

	return stream, nil
}

func (mgr *channelsMgrImpl) removeDMLStream(collectionID UniqueID) error {
	ids, err := mgr.getAllVIDs(collectionID)
	if err != nil {
		return err
	}

	mgr.deleteVChansByVIDs(ids)
	mgr.deleteDMLStreamByVIDs(ids)

	return nil
}

func (mgr *channelsMgrImpl) removeAllDMLStream() error {
	mgr.deleteAllChannels()
	mgr.deleteAllDMLStream()
	mgr.deleteAllVChans()
	mgr.deleteAllCollection()

	return nil
}

func newChannelsMgr(master masterService, factory msgstream.Factory) *channelsMgrImpl {
	return &channelsMgrImpl{
		collectionID2VIDs:            make(map[UniqueID][]int),
		id2vchans:                    make(map[int][]vChan),
		id2DMLStream:                 make(map[int]msgstream.MsgStream),
		id2UsageHistogramOfDMLStream: make(map[int]int),
		vchans2pchans:                make(map[vChan]pChan),
		master:                       master,
		msgStreamFactory:             factory,
	}
}
