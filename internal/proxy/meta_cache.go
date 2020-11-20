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

type MetaCache interface {
	Hit(collectionName string) bool
	Get(collectionName string) (*servicepb.CollectionDescription, error)
	Update(collectionName string) error
	//Write(collectionName string, schema *servicepb.CollectionDescription) error
}

var globalMetaCache MetaCache

type SimpleMetaCache struct {
	mu             sync.RWMutex
	metas          map[string]*servicepb.CollectionDescription // collection name to schema
	masterClient   masterpb.MasterClient
	reqIDAllocator *allocator.IDAllocator
	tsoAllocator   *allocator.TimestampAllocator
	ctx            context.Context
}

func (smc *SimpleMetaCache) Hit(collectionName string) bool {
	smc.mu.RLock()
	defer smc.mu.RUnlock()
	_, ok := smc.metas[collectionName]
	return ok
}

func (smc *SimpleMetaCache) Get(collectionName string) (*servicepb.CollectionDescription, error) {
	smc.mu.RLock()
	defer smc.mu.RUnlock()
	schema, ok := smc.metas[collectionName]
	if !ok {
		return nil, errors.New("collection meta miss")
	}
	return schema, nil
}

func (smc *SimpleMetaCache) Update(collectionName string) error {
	reqID, err := smc.reqIDAllocator.AllocOne()
	if err != nil {
		return err
	}
	ts, err := smc.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}
	req := &internalpb.DescribeCollectionRequest{
		MsgType:   internalpb.MsgType_kDescribeCollection,
		ReqID:     reqID,
		Timestamp: ts,
		ProxyID:   0,
		CollectionName: &servicepb.CollectionName{
			CollectionName: collectionName,
		},
	}

	resp, err := smc.masterClient.DescribeCollection(smc.ctx, req)
	if err != nil {
		return err
	}

	smc.mu.Lock()
	defer smc.mu.Unlock()
	smc.metas[collectionName] = resp

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
		ctx:            ctx,
	}
}

func initGlobalMetaCache(ctx context.Context,
	mCli masterpb.MasterClient,
	idAllocator *allocator.IDAllocator,
	tsoAllocator *allocator.TimestampAllocator) {
	globalMetaCache = newSimpleMetaCache(ctx, mCli, idAllocator, tsoAllocator)
}
