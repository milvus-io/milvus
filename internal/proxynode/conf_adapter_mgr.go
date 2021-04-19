package proxynode

import (
	"errors"
	"sync"
)

type ConfAdapterMgr interface {
	GetAdapter(indexType string) (ConfAdapter, error)
}

type ConfAdapterMgrImpl struct {
	init     bool
	adapters map[string]ConfAdapter
}

func (mgr *ConfAdapterMgrImpl) GetAdapter(indexType string) (ConfAdapter, error) {
	if !mgr.init {
		mgr.registerConfAdapter()
	}

	adapter, ok := mgr.adapters[indexType]
	if ok {
		return adapter, nil
	}
	return nil, errors.New("Can not find conf adapter: " + indexType)
}

func (mgr *ConfAdapterMgrImpl) registerConfAdapter() {
	mgr.init = true

	mgr.adapters[IndexFaissIdmap] = newBaseConfAdapter()
	mgr.adapters[IndexFaissIvfflat] = newIVFConfAdapter()
	mgr.adapters[IndexFaissIvfpq] = newIVFPQConfAdapter()
	mgr.adapters[IndexFaissIvfsq8] = newIVFSQConfAdapter()
	mgr.adapters[IndexFaissIvfsq8h] = newIVFSQConfAdapter()
	mgr.adapters[IndexFaissBinIdmap] = newBinIDMAPConfAdapter()
	mgr.adapters[IndexFaissBinIvfflat] = newBinIVFConfAdapter()
	mgr.adapters[IndexNsg] = newNSGConfAdapter()
	mgr.adapters[IndexHnsw] = newHNSWConfAdapter()
	mgr.adapters[IndexAnnoy] = newANNOYConfAdapter()
	mgr.adapters[IndexRhnswflat] = newRHNSWFlatConfAdapter()
	mgr.adapters[IndexRhnswpq] = newRHNSWPQConfAdapter()
	mgr.adapters[IndexRhnswsq] = newRHNSWSQConfAdapter()
	mgr.adapters[IndexNgtpanng] = newNGTPANNGConfAdapter()
	mgr.adapters[IndexNgtonng] = newNGTONNGConfAdapter()
}

func newConfAdapterMgrImpl() *ConfAdapterMgrImpl {
	return &ConfAdapterMgrImpl{}
}

var confAdapterMgr ConfAdapterMgr
var getConfAdapterMgrOnce sync.Once

func GetConfAdapterMgrInstance() ConfAdapterMgr {
	getConfAdapterMgrOnce.Do(func() {
		confAdapterMgr = newConfAdapterMgrImpl()
	})
	return confAdapterMgr
}
