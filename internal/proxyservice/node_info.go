package proxyservice

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"sync"

	grpcproxynodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/proxynode/client"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

type NodeInfo struct {
	ip   string
	port int64
}

type GlobalNodeInfoTable struct {
	mtx             sync.RWMutex
	nodeIDs         []UniqueID
	infos           map[UniqueID]*NodeInfo
	createClientMtx sync.RWMutex
	// lazy creating, so len(ProxyNodes) <= len(infos)
	ProxyNodes map[UniqueID]types.ProxyNode
}

func (table *GlobalNodeInfoTable) randomPick() UniqueID {
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
	log.Println("infos: ", table.infos)
	log.Println("ProxyNodes: ", table.ProxyNodes)
	if len(table.ProxyNodes) == len(table.infos) {
		return nil
	}

	for nodeID, info := range table.infos {
		_, ok := table.ProxyNodes[nodeID]
		if !ok {
			log.Println(info)
			table.ProxyNodes[nodeID] = grpcproxynodeclient.NewClient(context.Background(), info.ip+":"+strconv.Itoa(int(info.port)))
			var err error
			err = table.ProxyNodes[nodeID].Init()
			if err != nil {
				panic(err)
			}
			err = table.ProxyNodes[nodeID].Start()
			if err != nil {
				panic(err)
			}
		}
	}

	return nil
}

func (table *GlobalNodeInfoTable) ReleaseAllClients() error {
	table.createClientMtx.Lock()
	log.Println("get write lock")
	defer func() {
		table.createClientMtx.Unlock()
		log.Println("release write lock")
	}()

	var err error
	for id, client := range table.ProxyNodes {
		err = client.Stop()
		if err != nil {
			panic(err)
		}
		delete(table.ProxyNodes, id)
	}

	return nil
}

func (table *GlobalNodeInfoTable) ObtainAllClients() (map[UniqueID]types.ProxyNode, error) {
	table.mtx.RLock()
	defer table.mtx.RUnlock()

	table.createClientMtx.Lock()
	defer table.createClientMtx.Unlock()

	err := table.createClients()

	return table.ProxyNodes, err
}

func NewGlobalNodeInfoTable() *GlobalNodeInfoTable {
	return &GlobalNodeInfoTable{
		nodeIDs:    make([]UniqueID, 0),
		infos:      make(map[UniqueID]*NodeInfo),
		ProxyNodes: make(map[UniqueID]types.ProxyNode),
	}
}
