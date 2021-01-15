package proxynode

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type Cache interface {
	Hit(collectionName string) bool
	Get(collectionName string) (*servicepb.CollectionDescription, error)
	Sync(collectionName string) error
	Update(collectionName string, desc *servicepb.CollectionDescription) error
	Remove(collectionName string) error
}

var globalMetaCache Cache

type SimpleMetaCache struct {
	mu            sync.RWMutex
	metas         map[string]*servicepb.CollectionDescription // collection name to schema
	ctx           context.Context
	proxyInstance *Proxy
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

func (metaCache *SimpleMetaCache) Sync(collectionName string) error {
	dct := &DescribeCollectionTask{
		Condition: NewTaskCondition(metaCache.ctx),
		DescribeCollectionRequest: internalpb.DescribeCollectionRequest{
			MsgType: internalpb.MsgType_kDescribeCollection,
			CollectionName: &servicepb.CollectionName{
				CollectionName: collectionName,
			},
		},
		masterClient: metaCache.proxyInstance.masterClient,
	}
	var cancel func()
	dct.ctx, cancel = context.WithTimeout(metaCache.ctx, reqTimeoutInterval)
	defer cancel()

	err := metaCache.proxyInstance.sched.DdQueue.Enqueue(dct)
	if err != nil {
		return err
	}

	return dct.WaitToFinish()
}

func (metaCache *SimpleMetaCache) Update(collectionName string, desc *servicepb.CollectionDescription) error {
	metaCache.mu.Lock()
	defer metaCache.mu.Unlock()

	metaCache.metas[collectionName] = desc
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

func newSimpleMetaCache(ctx context.Context, proxyInstance *Proxy) *SimpleMetaCache {
	return &SimpleMetaCache{
		metas:         make(map[string]*servicepb.CollectionDescription),
		proxyInstance: proxyInstance,
		ctx:           ctx,
	}
}

func initGlobalMetaCache(ctx context.Context, proxyInstance *Proxy) {
	globalMetaCache = newSimpleMetaCache(ctx, proxyInstance)
}
