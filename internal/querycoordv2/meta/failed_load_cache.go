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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const expireTime = 24 * time.Hour

var GlobalFailedLoadCache *FailedLoadCache

type failInfo struct {
	count    int
	err      error
	lastTime time.Time
}

type FailedLoadCache struct {
	mu      sync.RWMutex
	records map[UniqueID]map[commonpb.ErrorCode]*failInfo
}

func NewFailedLoadCache() *FailedLoadCache {
	return &FailedLoadCache{
		records: make(map[UniqueID]map[commonpb.ErrorCode]*failInfo),
	}
}

func (l *FailedLoadCache) Get(collectionID UniqueID) *commonpb.Status {
	l.mu.RLock()
	defer l.mu.RUnlock()
	status := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	if _, ok := l.records[collectionID]; !ok {
		return status
	}
	if len(l.records[collectionID]) == 0 {
		return status
	}
	var max = 0
	for code, info := range l.records[collectionID] {
		if info.count > max {
			max = info.count
			status.ErrorCode = code
			status.Reason = info.err.Error()
		}
	}
	log.Warn("FailedLoadCache hits failed record", zap.Int64("collectionID", collectionID),
		zap.String("errCode", status.GetErrorCode().String()), zap.String("reason", status.GetReason()))
	return status
}

func (l *FailedLoadCache) Put(collectionID UniqueID, errCode commonpb.ErrorCode, err error) {
	if errCode == commonpb.ErrorCode_Success {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.records[collectionID]; !ok {
		l.records[collectionID] = make(map[commonpb.ErrorCode]*failInfo)
	}
	if _, ok := l.records[collectionID][errCode]; !ok {
		l.records[collectionID][errCode] = &failInfo{}
	}
	l.records[collectionID][errCode].count++
	l.records[collectionID][errCode].err = err
	l.records[collectionID][errCode].lastTime = time.Now()
	log.Warn("FailedLoadCache put failed record", zap.Int64("collectionID", collectionID),
		zap.String("errCode", errCode.String()), zap.Error(err))
}

func (l *FailedLoadCache) Remove(collectionID UniqueID) {
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
