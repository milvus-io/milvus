// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexparamcheck

import (
	"errors"
	"sync"
)

// ConfAdapterMgr manages the conf adapter.
type ConfAdapterMgr interface {
	// GetAdapter gets the conf adapter by the index type.
	GetAdapter(indexType string) (ConfAdapter, error)
}

// ConfAdapterMgrImpl implements ConfAdapter.
type ConfAdapterMgrImpl struct {
	adapters map[IndexType]ConfAdapter
	once     sync.Once
}

// GetAdapter gets the conf adapter by the index type.
func (mgr *ConfAdapterMgrImpl) GetAdapter(indexType string) (ConfAdapter, error) {
	mgr.once.Do(mgr.registerConfAdapter)

	adapter, ok := mgr.adapters[indexType]
	if ok {
		return adapter, nil
	}
	return nil, errors.New("Can not find conf adapter: " + indexType)
}

func (mgr *ConfAdapterMgrImpl) registerConfAdapter() {
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
		adapters: make(map[IndexType]ConfAdapter),
	}
}

var confAdapterMgr ConfAdapterMgr
var getConfAdapterMgrOnce sync.Once

// GetConfAdapterMgrInstance gets the instance of ConfAdapterMgr.
func GetConfAdapterMgrInstance() ConfAdapterMgr {
	getConfAdapterMgrOnce.Do(func() {
		confAdapterMgr = newConfAdapterMgrImpl()
	})
	return confAdapterMgr
}
