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

package meta

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const expireTime = 24 * time.Hour

var GlobalFailedLoadCache *FailedLoadCache

type failInfo struct {
	count    int
	err      error
	lastTime time.Time
}

type FailedLoadCache struct {
	mu sync.RWMutex
	// CollectionID, ErrorCode -> error
	records map[int64]map[int32]*failInfo
}

func NewFailedLoadCache() *FailedLoadCache {
	return &FailedLoadCache{
		records: make(map[int64]map[int32]*failInfo),
	}
}

func (l *FailedLoadCache) Get(collectionID int64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if _, ok := l.records[collectionID]; !ok {
		return nil
	}
	if len(l.records[collectionID]) == 0 {
		return nil
	}

	var (
		max = 0
		err error
	)
	for _, info := range l.records[collectionID] {
		if info.count > max {
			max = info.count
			err = info.err
		}
	}
	log.Warn("FailedLoadCache hits failed record",
		zap.Int64("collectionID", collectionID),
		zap.Error(err),
	)
	return err
}

func (l *FailedLoadCache) Put(collectionID int64, err error) {
	if err == nil {
		return
	}

	code := merr.Code(err)

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.records[collectionID]; !ok {
		l.records[collectionID] = make(map[int32]*failInfo)
	}
	if _, ok := l.records[collectionID][code]; !ok {
		l.records[collectionID][code] = &failInfo{}
	}
	l.records[collectionID][code].count++
	l.records[collectionID][code].err = err
	l.records[collectionID][code].lastTime = time.Now()
	log.Warn("FailedLoadCache put failed record",
		zap.Int64("collectionID", collectionID),
		zap.Error(err),
	)
}

func (l *FailedLoadCache) Remove(collectionID int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.records, collectionID)
	log.Info("FailedLoadCache removes cache", zap.Int64("collectionID", collectionID))
}

func (l *FailedLoadCache) TryExpire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for col, infos := range l.records {
		for code, info := range infos {
			if time.Since(info.lastTime) > expireTime {
				delete(l.records[col], code)
			}
		}
		if len(l.records[col]) == 0 {
			delete(l.records, col)
			log.Info("FailedLoadCache expires cache", zap.Int64("collectionID", col))
		}
	}
}
