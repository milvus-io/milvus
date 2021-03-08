package proxyservice

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"

	grpcproxynodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/proxynode/client"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

type NodeInfo struct {
	ip   string
	port int64
}

type GlobalNodeInfoTable struct {
	mu      sync.RWMutex
	infos   map[UniqueID]*NodeInfo
	nodeIDs []UniqueID
	// lazy creating, so len(clients) <= len(infos)
	ProxyNodes map[UniqueID]types.ProxyNode
}

func (table *GlobalNodeInfoTable) randomPick() UniqueID {
	l := len(table.nodeIDs)
	choice := rand.Intn(l)
	return table.nodeIDs[choice]
}

func (table *GlobalNodeInfoTable) Pick() (*NodeInfo, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()

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
	table.mu.Lock()
	defer table.mu.Unlock()

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
	if len(table.ProxyNodes) == len(table.infos) {
		return nil
	}

	for nodeID, info := range table.infos {
		_, ok := table.ProxyNodes[nodeID]
		if !ok {
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
	table.mu.Lock()
	log.Debug("get write lock")
	defer func() {
		table.mu.Unlock()
		log.Debug("release write lock")
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
	table.mu.RLock()
	defer table.mu.RUnlock()

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
