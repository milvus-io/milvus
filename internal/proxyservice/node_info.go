package proxyservice

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

type NodeInfo struct {
	ip   string
	port int64
}

// TODO: replace as real node client impl
type NodeClient interface {
	InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) error
}

type FakeNodeClient struct {
}

func (c *FakeNodeClient) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) error {
	panic("implement me")
}

type GlobalNodeInfoTable struct {
	mtx             sync.RWMutex
	nodeIDs         []UniqueID
	infos           map[UniqueID]*NodeInfo
	createClientMtx sync.RWMutex
	// lazy creating, so len(clients) <= len(infos)
	clients map[UniqueID]NodeClient
}

func (table *GlobalNodeInfoTable) randomPick() UniqueID {
	rand.Seed(time.Now().UnixNano())
	l := len(table.nodeIDs)
	choice := rand.Intn(l)
	return table.nodeIDs[choice]
}

func (table *GlobalNodeInfoTable) Pick() (*NodeInfo, error) {
	table.mtx.RLock()
	defer table.mtx.RUnlock()

	if len(table.nodeIDs) <= 0 || len(table.infos) <= 0 {
		return nil, errors.New("no available server node")
	}

	id := table.randomPick()
	info, ok := table.infos[id]
	if !ok {
		// though impossible
		return nil, errors.New("fix me, something wrong in pick algorithm")
	}

	return info, nil
}

func (table *GlobalNodeInfoTable) Register(id UniqueID, info *NodeInfo) error {
	table.mtx.Lock()
	defer table.mtx.Unlock()

	_, ok := table.infos[id]
	if !ok {
		table.infos[id] = info
	}

	if !SliceContain(table.nodeIDs, id) {
		table.nodeIDs = append(table.nodeIDs, id)
	}

	return nil
}

func (table *GlobalNodeInfoTable) createClients() error {
	if len(table.clients) == len(table.infos) {
		return nil
	}

	for nodeID, info := range table.infos {
		_, ok := table.clients[nodeID]
		if !ok {
			// TODO: use info to create client
			fmt.Println(info)
			table.clients[nodeID] = &FakeNodeClient{}
		}
	}

	return nil
}

func (table *GlobalNodeInfoTable) ObtainAllClients() (map[UniqueID]NodeClient, error) {
	table.mtx.RLock()
	defer table.mtx.RUnlock()

	table.createClientMtx.Lock()
	defer table.createClientMtx.Unlock()

	err := table.createClients()

	return table.clients, err
}

func NewGlobalNodeInfoTable() *GlobalNodeInfoTable {
	return &GlobalNodeInfoTable{
		nodeIDs: make([]UniqueID, 0),
		infos:   make(map[UniqueID]*NodeInfo),
		clients: make(map[UniqueID]NodeClient),
	}
}
