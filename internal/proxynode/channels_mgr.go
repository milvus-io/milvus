package proxynode

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"go.uber.org/zap"
)

type vChan = string
type pChan = string

type channelsMgr interface {
	getChannels(collectionID UniqueID) ([]pChan, error)
	getVChannels(collectionID UniqueID) ([]vChan, error)
	createDQLStream(collectionID UniqueID) error
	getDQLStream(collectionID UniqueID) (msgstream.MsgStream, error)
	removeDQLStream(collectionID UniqueID) error
	removeAllDQLStream() error
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

type getChannelsFuncType = func(collectionID UniqueID) (map[vChan]pChan, error)

type getChannelsService interface {
	GetChannels(collectionID UniqueID) (map[vChan]pChan, error)
}

type mockGetChannelsService struct {
	collectionID2Channels map[UniqueID]map[vChan]pChan
}

func newMockGetChannelsService() *mockGetChannelsService {
	return &mockGetChannelsService{
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

func (m *mockGetChannelsService) GetChannels(collectionID UniqueID) (map[vChan]pChan, error) {
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

type singleTypeChannelsMgr struct {
	collectionID2VIDs map[UniqueID][]int // id are sorted
	collMtx           sync.RWMutex

	id2vchans    map[int][]vChan
	id2vchansMtx sync.RWMutex

	id2Stream                 map[int]msgstream.MsgStream
	id2UsageHistogramOfStream map[int]int
	streamMtx                 sync.RWMutex

	vchans2pchans    map[vChan]pChan
	vchans2pchansMtx sync.RWMutex

	getChannelsFunc getChannelsFuncType

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

func (mgr *singleTypeChannelsMgr) getLatestVID(collectionID UniqueID) (int, error) {
	mgr.collMtx.RLock()
	defer mgr.collMtx.RUnlock()

	ids, ok := mgr.collectionID2VIDs[collectionID]
	if !ok || len(ids) <= 0 {
		return 0, fmt.Errorf("collection %d not found", collectionID)
	}

	return ids[len(ids)-1], nil
}

func (mgr *singleTypeChannelsMgr) getAllVIDs(collectionID UniqueID) ([]int, error) {
	mgr.collMtx.RLock()
	defer mgr.collMtx.RUnlock()

	return mgr.collectionID2VIDs[collectionID], nil
}

func (mgr *singleTypeChannelsMgr) getVChansByVID(vid int) ([]vChan, error) {
	mgr.id2vchansMtx.RLock()
	defer mgr.id2vchansMtx.RUnlock()

	vchans, ok := mgr.id2vchans[vid]
	if !ok {
		return nil, fmt.Errorf("vid %d not found", vid)
	}

	return vchans, nil
}

func (mgr *singleTypeChannelsMgr) getPChansByVChans(vchans []vChan) ([]pChan, error) {
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

func (mgr *singleTypeChannelsMgr) updateVChans(vid int, vchans []vChan) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2vchans[vid] = vchans
}

func (mgr *singleTypeChannelsMgr) deleteVChansByVID(vid int) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	delete(mgr.id2vchans, vid)
}

func (mgr *singleTypeChannelsMgr) deleteVChansByVIDs(vids []int) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	for _, vid := range vids {
		delete(mgr.id2vchans, vid)
	}
}

func (mgr *singleTypeChannelsMgr) deleteStreamByVID(vid int) {
	mgr.streamMtx.Lock()
	defer mgr.streamMtx.Unlock()

	delete(mgr.id2Stream, vid)
}

func (mgr *singleTypeChannelsMgr) deleteStreamByVIDs(vids []int) {
	mgr.streamMtx.Lock()
	defer mgr.streamMtx.Unlock()

	for _, vid := range vids {
		delete(mgr.id2Stream, vid)
	}
}

func (mgr *singleTypeChannelsMgr) updateChannels(channels map[vChan]pChan) {
	mgr.vchans2pchansMtx.Lock()
	defer mgr.vchans2pchansMtx.Unlock()

	for vchan, pchan := range channels {
		mgr.vchans2pchans[vchan] = pchan
	}
}

func (mgr *singleTypeChannelsMgr) deleteAllChannels() {
	mgr.vchans2pchansMtx.Lock()
	defer mgr.vchans2pchansMtx.Unlock()

	mgr.vchans2pchans = nil
}

func (mgr *singleTypeChannelsMgr) deleteAllStream() {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2UsageHistogramOfStream = nil
	mgr.id2Stream = nil
}

func (mgr *singleTypeChannelsMgr) deleteAllVChans() {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2vchans = nil
}

func (mgr *singleTypeChannelsMgr) deleteAllCollection() {
	mgr.collMtx.Lock()
	defer mgr.collMtx.Unlock()

	mgr.collectionID2VIDs = nil
}

func (mgr *singleTypeChannelsMgr) addStream(vid int, stream msgstream.MsgStream) {
	mgr.streamMtx.Lock()
	defer mgr.streamMtx.Unlock()

	mgr.id2Stream[vid] = stream
	mgr.id2UsageHistogramOfStream[vid] = 0
}

func (mgr *singleTypeChannelsMgr) updateCollection(collectionID UniqueID, id int) {
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

func (mgr *singleTypeChannelsMgr) getChannels(collectionID UniqueID) ([]pChan, error) {
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

func (mgr *singleTypeChannelsMgr) getVChannels(collectionID UniqueID) ([]vChan, error) {
	id, err := mgr.getLatestVID(collectionID)
	if err == nil {
		return mgr.getVChansByVID(id)
	}

	// TODO(dragondriver): return error or update channel information from master?
	return nil, err
}

func (mgr *singleTypeChannelsMgr) createMsgStream(collectionID UniqueID) error {
	channels, err := mgr.getChannelsFunc(collectionID)
	log.Debug("singleTypeChannelsMgr", zap.Any("createMsgStream.getChannels", channels))
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
		// after assigning segment id to msg, tsMsgs was already re-bucketed
		pack := make(map[int32]*msgstream.MsgPack)
		for idx, msg := range tsMsgs {
			if len(hashKeys[idx]) <= 0 {
				continue
			}
			key := hashKeys[idx][0]
			_, ok := pack[key]
			if !ok {
				pack[key] = &msgstream.MsgPack{}
			}
			pack[key].Msgs = append(pack[key].Msgs, msg)
		}
		return pack, nil
	}
	stream.SetRepackFunc(repack)
	runtime.SetFinalizer(stream, func(stream msgstream.MsgStream) {
		stream.Close()
	})
	mgr.addStream(id, stream)

	mgr.updateCollection(collectionID, id)

	return nil
}

func (mgr *singleTypeChannelsMgr) getStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	mgr.streamMtx.RLock()
	defer mgr.streamMtx.RUnlock()

	vid, err := mgr.getLatestVID(collectionID)
	if err != nil {
		return nil, err
	}

	stream, ok := mgr.id2Stream[vid]
	if !ok {
		return nil, fmt.Errorf("no dml stream for collection %v", collectionID)
	}

	return stream, nil
}

func (mgr *singleTypeChannelsMgr) removeStream(collectionID UniqueID) error {
	ids, err := mgr.getAllVIDs(collectionID)
	if err != nil {
		return err
	}

	mgr.deleteVChansByVIDs(ids)
	mgr.deleteStreamByVIDs(ids)

	return nil
}

func (mgr *singleTypeChannelsMgr) removeAllStream() error {
	mgr.deleteAllChannels()
	mgr.deleteAllStream()
	mgr.deleteAllVChans()
	mgr.deleteAllCollection()

	return nil
}

func newSingleTypeChannelsMgr(getChannelsFunc getChannelsFuncType, msgStreamFactory msgstream.Factory) *singleTypeChannelsMgr {
	return &singleTypeChannelsMgr{
		collectionID2VIDs:         make(map[UniqueID][]int),
		id2vchans:                 make(map[int][]vChan),
		id2Stream:                 make(map[int]msgstream.MsgStream),
		id2UsageHistogramOfStream: make(map[int]int),
		vchans2pchans:             make(map[vChan]pChan),
		getChannelsFunc:           getChannelsFunc,
		msgStreamFactory:          msgStreamFactory,
	}
}

type channelsMgrImpl struct {
	dmlChannelsMgr *singleTypeChannelsMgr
	dqlChannelsMgr *singleTypeChannelsMgr
}

func (mgr *channelsMgrImpl) getChannels(collectionID UniqueID) ([]pChan, error) {
	return mgr.dmlChannelsMgr.getChannels(collectionID)
}

func (mgr *channelsMgrImpl) getVChannels(collectionID UniqueID) ([]vChan, error) {
	return mgr.dmlChannelsMgr.getVChannels(collectionID)
}

func (mgr *channelsMgrImpl) createDQLStream(collectionID UniqueID) error {
	return mgr.dqlChannelsMgr.createMsgStream(collectionID)
}

func (mgr *channelsMgrImpl) getDQLStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	return mgr.dqlChannelsMgr.getStream(collectionID)
}

func (mgr *channelsMgrImpl) removeDQLStream(collectionID UniqueID) error {
	return mgr.dqlChannelsMgr.removeStream(collectionID)
}

func (mgr *channelsMgrImpl) removeAllDQLStream() error {
	return mgr.dqlChannelsMgr.removeAllStream()
}

func (mgr *channelsMgrImpl) createDMLMsgStream(collectionID UniqueID) error {
	return mgr.dmlChannelsMgr.createMsgStream(collectionID)
}

func (mgr *channelsMgrImpl) getDMLStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	return mgr.dmlChannelsMgr.getStream(collectionID)
}

func (mgr *channelsMgrImpl) removeDMLStream(collectionID UniqueID) error {
	return mgr.dmlChannelsMgr.removeStream(collectionID)
}

func (mgr *channelsMgrImpl) removeAllDMLStream() error {
	return mgr.dmlChannelsMgr.removeAllStream()
}

func newChannelsMgr(getDmlChannelsFunc getChannelsFuncType, getDqlChannelsFunc getChannelsFuncType, msgStreamFactory msgstream.Factory) channelsMgr {
	return &channelsMgrImpl{
		dmlChannelsMgr: newSingleTypeChannelsMgr(getDmlChannelsFunc, msgStreamFactory),
		dqlChannelsMgr: newSingleTypeChannelsMgr(getDqlChannelsFunc, msgStreamFactory),
	}
}
