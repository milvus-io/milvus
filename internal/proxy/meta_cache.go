package proxy

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type Cache interface {
	Hit(collectionName string) bool
	Get(collectionName string) (*servicepb.CollectionDescription, error)
	Update(collectionName string) error
	Remove(collectionName string) error
}

var globalMetaCache Cache

type SimpleMetaCache struct {
	mu             sync.RWMutex
	proxyID        UniqueID
	metas          map[string]*servicepb.CollectionDescription // collection name to schema
	masterClient   masterpb.MasterClient
	reqIDAllocator *allocator.IDAllocator
	tsoAllocator   *allocator.TimestampAllocator
	ctx            context.Context
}

func (metaCache *SimpleMetaCache) Hit(collectionName string) bool {
	metaCache.mu.RLock()
	defer metaCache.mu.RUnlock()
	_, ok := metaCache.metas[collectionName]
	return ok
}

func (metaCache *SimpleMetaCache) Get(collectionName string) (*servicepb.CollectionDescription, error) {
	metaCache.mu.RLock()
	defer metaCache.mu.RUnlock()
	schema, ok := metaCache.metas[collectionName]
	if !ok {
		return nil, errors.New("collection meta miss")
	}
	return schema, nil
}

func (metaCache *SimpleMetaCache) Update(collectionName string) error {
	reqID, err := metaCache.reqIDAllocator.AllocOne()
	if err != nil {
		return err
	}
	ts, err := metaCache.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}
	hasCollectionReq := &internalpb.HasCollectionRequest{
		MsgType:   internalpb.MsgType_kHasCollection,
		ReqID:     reqID,
		Timestamp: ts,
		ProxyID:   metaCache.proxyID,
		CollectionName: &servicepb.CollectionName{
			CollectionName: collectionName,
		},
	}
	has, err := metaCache.masterClient.HasCollection(metaCache.ctx, hasCollectionReq)
	if err != nil {
		return err
	}
	if !has.Value {
		return errors.New("collection " + collectionName + " not exists")
	}

	reqID, err = metaCache.reqIDAllocator.AllocOne()
	if err != nil {
		return err
	}
	ts, err = metaCache.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}
	req := &internalpb.DescribeCollectionRequest{
		MsgType:   internalpb.MsgType_kDescribeCollection,
		ReqID:     reqID,
		Timestamp: ts,
		ProxyID:   metaCache.proxyID,
		CollectionName: &servicepb.CollectionName{
			CollectionName: collectionName,
		},
	}
	resp, err := metaCache.masterClient.DescribeCollection(metaCache.ctx, req)
	if err != nil {
		return err
	}

	metaCache.mu.Lock()
	defer metaCache.mu.Unlock()
	metaCache.metas[collectionName] = resp

	return nil
}

func (metaCache *SimpleMetaCache) Remove(collectionName string) error {
	metaCache.mu.Lock()
	defer metaCache.mu.Unlock()

	_, ok := metaCache.metas[collectionName]
	if !ok {
		return errors.New("cannot find collection: " + collectionName)
	}
	delete(metaCache.metas, collectionName)

	return nil
}

func newSimpleMetaCache(ctx context.Context,
	mCli masterpb.MasterClient,
	idAllocator *allocator.IDAllocator,
	tsoAllocator *allocator.TimestampAllocator) *SimpleMetaCache {
	return &SimpleMetaCache{
		metas:          make(map[string]*servicepb.CollectionDescription),
		masterClient:   mCli,
		reqIDAllocator: idAllocator,
		tsoAllocator:   tsoAllocator,
		proxyID:        Params.ProxyID(),
		ctx:            ctx,
	}
}

func initGlobalMetaCache(ctx context.Context,
	mCli masterpb.MasterClient,
	idAllocator *allocator.IDAllocator,
	tsoAllocator *allocator.TimestampAllocator) {
	globalMetaCache = newSimpleMetaCache(ctx, mCli, idAllocator, tsoAllocator)
}
