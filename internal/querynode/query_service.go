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

package querynode

import "C"
import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
)

type queryService struct {
	ctx    context.Context
	cancel context.CancelFunc

	historical *historical
	streaming  *streaming

	queryCollectionMu sync.Mutex // guards queryCollections
	queryCollections  map[UniqueID]*queryCollection

	factory msgstream.Factory

	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	localCacheEnabled  bool
}

func newQueryService(ctx context.Context,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory) *queryService {

	queryServiceCtx, queryServiceCancel := context.WithCancel(ctx)

	//TODO godchen: change this to configuration
	path, err := Params.BaseParams.Load("localStorage.Path")
	if err != nil {
		path = "/tmp/milvus/data"
	}
	enabled, _ := Params.BaseParams.Load("localStorage.enabled")
	localCacheEnabled, _ := strconv.ParseBool(enabled)

	localChunkManager := storage.NewLocalChunkManager(path)

	option := &miniokv.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	client, err := miniokv.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}
	remoteChunkManager := storage.NewMinioChunkManager(client)

	return &queryService{
		ctx:    queryServiceCtx,
		cancel: queryServiceCancel,

		historical: historical,
		streaming:  streaming,

		queryCollections: make(map[UniqueID]*queryCollection),

		factory: factory,

		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,
		localCacheEnabled:  localCacheEnabled,
	}
}

func (q *queryService) close() {
	log.Debug("search service closed")
	q.queryCollectionMu.Lock()
	for collectionID, sc := range q.queryCollections {
		sc.close()
		sc.cancel()
		delete(q.queryCollections, collectionID)
	}
	q.queryCollections = make(map[UniqueID]*queryCollection)
	q.queryCollectionMu.Unlock()
	q.cancel()
}

func (q *queryService) addQueryCollection(collectionID UniqueID) error {
	q.queryCollectionMu.Lock()
	defer q.queryCollectionMu.Unlock()
	if _, ok := q.queryCollections[collectionID]; ok {
		log.Warn("query collection already exists", zap.Any("collectionID", collectionID))
		err := errors.New(fmt.Sprintln("query collection already exists, collectionID = ", collectionID))
		return err
	}
	ctx1, cancel := context.WithCancel(q.ctx)
	qc, err := newQueryCollection(ctx1,
		cancel,
		collectionID,
		q.historical,
		q.streaming,
		q.factory,
		q.localChunkManager,
		q.remoteChunkManager,
		q.localCacheEnabled,
	)
	if err != nil {
		return err
	}
	q.queryCollections[collectionID] = qc
	return nil
}

func (q *queryService) hasQueryCollection(collectionID UniqueID) bool {
	q.queryCollectionMu.Lock()
	defer q.queryCollectionMu.Unlock()
	_, ok := q.queryCollections[collectionID]
	return ok
}

func (q *queryService) getQueryCollection(collectionID UniqueID) (*queryCollection, error) {
	q.queryCollectionMu.Lock()
	defer q.queryCollectionMu.Unlock()
	_, ok := q.queryCollections[collectionID]
	if ok {
		return q.queryCollections[collectionID], nil
	}
	return nil, errors.New(fmt.Sprintln("queryCollection not exists, collectionID = ", collectionID))
}

func (q *queryService) stopQueryCollection(collectionID UniqueID) {
	q.queryCollectionMu.Lock()
	defer q.queryCollectionMu.Unlock()
	sc, ok := q.queryCollections[collectionID]
	if !ok {
		log.Warn("stopQueryCollection failed, collection doesn't exist", zap.Int64("collectionID", collectionID))
		return
	}
	sc.close()
	sc.cancel()
	// for not blocking waitNewTsafe, which will block doUnsolvedMsg quit.
	sc.watcherCond.Broadcast()
	delete(q.queryCollections, collectionID)
}
