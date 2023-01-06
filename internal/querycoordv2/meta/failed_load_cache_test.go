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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
)

func TestFailedLoadCache(t *testing.T) {
	GlobalFailedLoadCache = NewFailedLoadCache()

	colID := int64(0)
	errCode := commonpb.ErrorCode_InsufficientMemoryToLoad
	mockErr := fmt.Errorf("mock insufficient memory reason")

	GlobalFailedLoadCache.Put(colID, commonpb.ErrorCode_Success, nil)
	res := GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, commonpb.ErrorCode_Success, res.GetErrorCode())

	GlobalFailedLoadCache.Put(colID, errCode, mockErr)
	res = GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, errCode, res.GetErrorCode())

	GlobalFailedLoadCache.Remove(colID)
	res = GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, commonpb.ErrorCode_Success, res.GetErrorCode())

	GlobalFailedLoadCache.Put(colID, errCode, mockErr)
	GlobalFailedLoadCache.mu.Lock()
	GlobalFailedLoadCache.records[colID][errCode].lastTime = time.Now().Add(-expireTime * 2)
	GlobalFailedLoadCache.mu.Unlock()
	GlobalFailedLoadCache.TryExpire()
	res = GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, commonpb.ErrorCode_Success, res.GetErrorCode())
}
