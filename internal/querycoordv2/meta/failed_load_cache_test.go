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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestFailedLoadCache(t *testing.T) {
	GlobalFailedLoadCache = NewFailedLoadCache()

	colID := int64(0)
	mockErr := merr.WrapErrServiceMemoryLimitExceeded(0, 0)

	GlobalFailedLoadCache.Put(colID, nil)
	err := GlobalFailedLoadCache.Get(colID)
	assert.NoError(t, err)

	GlobalFailedLoadCache.Put(colID, mockErr)
	err = GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, merr.Code(merr.ErrServiceMemoryLimitExceeded), merr.Code(err))

	GlobalFailedLoadCache.Remove(colID)
	err = GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, commonpb.ErrorCode_Success, merr.Status(err).ErrorCode)

	GlobalFailedLoadCache.Put(colID, mockErr)
	GlobalFailedLoadCache.mu.Lock()
	GlobalFailedLoadCache.records[colID][merr.Code(mockErr)].lastTime = time.Now().Add(-expireTime * 2)
	GlobalFailedLoadCache.mu.Unlock()
	GlobalFailedLoadCache.TryExpire()
	err = GlobalFailedLoadCache.Get(colID)
	assert.Equal(t, commonpb.ErrorCode_Success, merr.Status(err).ErrorCode)
}
