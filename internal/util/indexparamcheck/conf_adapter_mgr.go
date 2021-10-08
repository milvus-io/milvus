// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexparamcheck

import (
	"errors"
	"sync"
)

type ConfAdapterMgr interface {
	GetAdapter(indexType string) (ConfAdapter, error)
}

// ConfAdapterMgrImpl implements ConfAdapter
type ConfAdapterMgrImpl struct {
	init     bool
	adapters map[IndexType]ConfAdapter
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

	mgr.adapters[IndexFaissIDMap] = newBaseConfAdapter()
	mgr.adapters[IndexFaissIvfFlat] = newIVFConfAdapter()
	mgr.adapters[IndexFaissIvfPQ] = newIVFPQConfAdapter()
	mgr.adapters[IndexFaissIvfSQ8] = newIVFSQConfAdapter()
	mgr.adapters[IndexFaissIvfSQ8H] = newIVFSQConfAdapter()
	mgr.adapters[IndexFaissBinIDMap] = newBinIDMAPConfAdapter()
	mgr.adapters[IndexFaissBinIvfFlat] = newBinIVFConfAdapter()
	mgr.adapters[IndexNSG] = newNSGConfAdapter()
	mgr.adapters[IndexHNSW] = newHNSWConfAdapter()
	mgr.adapters[IndexANNOY] = newANNOYConfAdapter()
	mgr.adapters[IndexRHNSWFlat] = newRHNSWFlatConfAdapter()
	mgr.adapters[IndexRHNSWPQ] = newRHNSWPQConfAdapter()
	mgr.adapters[IndexRHNSWSQ] = newRHNSWSQConfAdapter()
	mgr.adapters[IndexNGTPANNG] = newNGTPANNGConfAdapter()
	mgr.adapters[IndexNGTONNG] = newNGTONNGConfAdapter()
}

func newConfAdapterMgrImpl() *ConfAdapterMgrImpl {
	return &ConfAdapterMgrImpl{
		init:     false,
		adapters: make(map[IndexType]ConfAdapter),
	}
}

var confAdapterMgr ConfAdapterMgr
var getConfAdapterMgrOnce sync.Once

func GetConfAdapterMgrInstance() ConfAdapterMgr {
	getConfAdapterMgrOnce.Do(func() {
		confAdapterMgr = newConfAdapterMgrImpl()
	})
	return confAdapterMgr
}
