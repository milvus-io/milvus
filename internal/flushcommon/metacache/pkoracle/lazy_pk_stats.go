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

package pkoracle

import (
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

var _ PkStat = (*LazyPkStats)(nil)

// LazyPkStats
type LazyPkStats struct {
	inner atomic.Pointer[PkStat]
}

func NewLazyPkstats() *LazyPkStats {
	return &LazyPkStats{}
}

func (s *LazyPkStats) SetPkStats(pk PkStat) {
	if pk != nil {
		s.inner.Store(&pk)
	}
}

func (s *LazyPkStats) PkExists(lc *storage.LocationsCache) bool {
	inner := s.inner.Load()
	if inner == nil {
		return true
	}
	return (*inner).PkExists(lc)
}

func (s *LazyPkStats) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	inner := s.inner.Load()
	if inner == nil {
		return lo.RepeatBy(lc.Size(), func(_ int) bool {
			return true
		})
	}
	return (*inner).BatchPkExist(lc)
}

func (s *LazyPkStats) BatchPkExistWithHits(lc *storage.BatchLocationsCache, hits []bool) []bool {
	inner := s.inner.Load()
	if inner == nil {
		return lo.RepeatBy(lc.Size(), func(_ int) bool {
			return true
		})
	}
	return (*inner).BatchPkExistWithHits(lc, hits)
}

func (s *LazyPkStats) UpdatePKRange(ids storage.FieldData) error {
	return merr.WrapErrServiceInternal("UpdatePKRange shall never be called on LazyPkStats")
}

func (s *LazyPkStats) Roll(newStats ...*storage.PrimaryKeyStats) {
	merr.WrapErrServiceInternal("Roll shall never be called on LazyPkStats")
}

func (s *LazyPkStats) GetHistory() []*storage.PkStatistics {
	// GetHistory shall never be called on LazyPkStats
	return nil
}
