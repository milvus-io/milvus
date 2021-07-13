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

package querynode

import "C"
import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
)

type queryService struct {
	ctx    context.Context
	cancel context.CancelFunc

	historical *historical
	streaming  *streaming

	queryCollections map[UniqueID]*queryCollection

	factory msgstream.Factory
}

func newQueryService(ctx context.Context,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory) *queryService {

	queryServiceCtx, queryServiceCancel := context.WithCancel(ctx)
	return &queryService{
		ctx:    queryServiceCtx,
		cancel: queryServiceCancel,

		historical: historical,
		streaming:  streaming,

		queryCollections: make(map[UniqueID]*queryCollection),

		factory: factory,
	}
}

func (q *queryService) close() {
	log.Debug("search service closed")
	for collectionID := range q.queryCollections {
		q.stopQueryCollection(collectionID)
	}
	q.queryCollections = make(map[UniqueID]*queryCollection)
	q.cancel()
}

func (q *queryService) addQueryCollection(collectionID UniqueID) {
	if _, ok := q.queryCollections[collectionID]; ok {
		log.Warn("query collection already exists", zap.Any("collectionID", collectionID))
		return
	}

	ctx1, cancel := context.WithCancel(q.ctx)
	qc := newQueryCollection(ctx1,
		cancel,
		collectionID,
		q.historical,
		q.streaming,
		q.factory)
	q.queryCollections[collectionID] = qc
}

func (q *queryService) hasQueryCollection(collectionID UniqueID) bool {
	_, ok := q.queryCollections[collectionID]
	return ok
}

func (q *queryService) stopQueryCollection(collectionID UniqueID) {
	sc, ok := q.queryCollections[collectionID]
	if !ok {
		log.Error("stopQueryCollection failed, collection doesn't exist", zap.Int64("collectionID", collectionID))
		return
	}
	sc.close()
	sc.cancel()
	delete(q.queryCollections, collectionID)
}
