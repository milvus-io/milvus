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

type searchService struct {
	ctx    context.Context
	cancel context.CancelFunc

	historical *historical
	streaming  *streaming

	queryNodeID       UniqueID
	searchCollections map[UniqueID]*searchCollection

	factory msgstream.Factory
}

func newSearchService(ctx context.Context,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory) *searchService {

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	return &searchService{
		ctx:    searchServiceCtx,
		cancel: searchServiceCancel,

		historical: historical,
		streaming:  streaming,

		queryNodeID:       Params.QueryNodeID,
		searchCollections: make(map[UniqueID]*searchCollection),

		factory: factory,
	}
}

func (s *searchService) close() {
	log.Debug("search service closed")
	for collectionID := range s.searchCollections {
		s.stopSearchCollection(collectionID)
	}
	s.searchCollections = make(map[UniqueID]*searchCollection)
	s.cancel()
}

func (s *searchService) addSearchCollection(collectionID UniqueID) {
	if _, ok := s.searchCollections[collectionID]; ok {
		log.Warn("search collection already exists", zap.Any("collectionID", collectionID))
		return
	}

	ctx1, cancel := context.WithCancel(s.ctx)
	sc := newSearchCollection(ctx1,
		cancel,
		collectionID,
		s.historical,
		s.streaming,
		s.factory)
	s.searchCollections[collectionID] = sc
}

func (s *searchService) hasSearchCollection(collectionID UniqueID) bool {
	_, ok := s.searchCollections[collectionID]
	return ok
}

func (s *searchService) stopSearchCollection(collectionID UniqueID) {
	sc, ok := s.searchCollections[collectionID]
	if !ok {
		log.Error("stopSearchCollection failed, collection doesn't exist", zap.Int64("collectionID", collectionID))
		return
	}
	sc.close()
	sc.cancel()
	delete(s.searchCollections, collectionID)
}
