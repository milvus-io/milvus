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

	"github.com/milvus-io/milvus/internal/util/typedef"
)

type ConfAdapterMgr interface {
	GetAdapter(indexType string) (ConfAdapter, error)
}

type ConfAdapterMgrImpl struct {
	init     bool
	adapters map[typedef.IndexType]ConfAdapter
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

	mgr.adapters[typedef.IndexFaissIDMap] = newBaseConfAdapter()
	mgr.adapters[typedef.IndexFaissIvfFlat] = newIVFConfAdapter()
	mgr.adapters[typedef.IndexFaissIvfPQ] = newIVFPQConfAdapter()
	mgr.adapters[typedef.IndexFaissIvfSQ8] = newIVFSQConfAdapter()
	mgr.adapters[typedef.IndexFaissIvfSQ8H] = newIVFSQConfAdapter()
	mgr.adapters[typedef.IndexFaissBinIDMap] = newBinIDMAPConfAdapter()
	mgr.adapters[typedef.IndexFaissBinIvfFlat] = newBinIVFConfAdapter()
	mgr.adapters[typedef.IndexNSG] = newNSGConfAdapter()
	mgr.adapters[typedef.IndexHNSW] = newHNSWConfAdapter()
	mgr.adapters[typedef.IndexANNOY] = newANNOYConfAdapter()
	mgr.adapters[typedef.IndexRHNSWFlat] = newRHNSWFlatConfAdapter()
	mgr.adapters[typedef.IndexRHNSWPQ] = newRHNSWPQConfAdapter()
	mgr.adapters[typedef.IndexRHNSWSQ] = newRHNSWSQConfAdapter()
	mgr.adapters[typedef.IndexNGTPANNG] = newNGTPANNGConfAdapter()
	mgr.adapters[typedef.IndexNGTONNG] = newNGTONNGConfAdapter()
}

func newConfAdapterMgrImpl() *ConfAdapterMgrImpl {
	return &ConfAdapterMgrImpl{
		init:     false,
		adapters: make(map[typedef.IndexType]ConfAdapter),
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
