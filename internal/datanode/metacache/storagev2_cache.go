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

package metacache

import (
	"sync"

	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type StorageV2Cache struct {
	arrowSchema *arrow.Schema
	spaceMu     sync.Mutex
	spaces      map[int64]*milvus_storage.Space
}

func (s *StorageV2Cache) ArrowSchema() *arrow.Schema {
	return s.arrowSchema
}

func (s *StorageV2Cache) GetOrCreateSpace(segmentID int64, creator func() (*milvus_storage.Space, error)) (*milvus_storage.Space, error) {
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	space, ok := s.spaces[segmentID]
	if ok {
		return space, nil
	}
	space, err := creator()
	if err != nil {
		return nil, err
	}
	s.spaces[segmentID] = space
	return space, nil
}

// only for unit test
func (s *StorageV2Cache) SetSpace(segmentID int64, space *milvus_storage.Space) {
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	s.spaces[segmentID] = space
}

func NewStorageV2Cache(schema *schemapb.CollectionSchema) (*StorageV2Cache, error) {
	arrowSchema, err := typeutil.ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, err
	}
	return &StorageV2Cache{
		arrowSchema: arrowSchema,
		spaces:      make(map[int64]*milvus_storage.Space),
	}, nil
}
