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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetConfAdapterMgrInstance(t *testing.T) {
	adapterMgr := GetConfAdapterMgrInstance()

	var adapter ConfAdapter
	var err error
	var ok bool

	adapter, err = adapterMgr.GetAdapter("invalid")
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, adapter)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIDMap)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*BaseConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfFlat)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfPQ)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFPQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfSQ8)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFSQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfSQ8H)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFSQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissBinIDMap)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*BinIDMAPConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissBinIvfFlat)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*BinIVFConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexNSG)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*NSGConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexHNSW)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*HNSWConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexRHNSWFlat)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*RHNSWFlatConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexRHNSWPQ)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*RHNSWPQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexRHNSWSQ)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*RHNSWSQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexANNOY)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*ANNOYConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexNGTPANNG)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*NGTPANNGConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexNGTONNG)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*NGTONNGConfAdapter)
	assert.Equal(t, true, ok)
}

func TestConfAdapterMgrImpl_GetAdapter(t *testing.T) {
	adapterMgr := newConfAdapterMgrImpl()

	var adapter ConfAdapter
	var err error
	var ok bool

	adapter, err = adapterMgr.GetAdapter("invalid")
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, adapter)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIDMap)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*BaseConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfFlat)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfPQ)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFPQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfSQ8)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFSQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissIvfSQ8H)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*IVFSQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissBinIDMap)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*BinIDMAPConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexFaissBinIvfFlat)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*BinIVFConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexNSG)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*NSGConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexHNSW)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*HNSWConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexRHNSWFlat)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*RHNSWFlatConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexRHNSWPQ)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*RHNSWPQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexRHNSWSQ)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*RHNSWSQConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexANNOY)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*ANNOYConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexNGTPANNG)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*NGTPANNGConfAdapter)
	assert.Equal(t, true, ok)

	adapter, err = adapterMgr.GetAdapter(IndexNGTONNG)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, adapter)
	_, ok = adapter.(*NGTONNGConfAdapter)
	assert.Equal(t, true, ok)
}
