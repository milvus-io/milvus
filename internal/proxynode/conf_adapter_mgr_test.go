package proxynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetConfAdapterMgrInstance(t *testing.T) {
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
