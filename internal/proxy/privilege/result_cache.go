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

package privilege

import (
	"fmt"
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	priCacheInitOnce sync.Once
	priCacheMut      sync.RWMutex
	priCache         *resultCache
	ver              atomic.Int64
)

func getPriCache() *resultCache {
	priCacheMut.RLock()
	c := priCache
	priCacheMut.RUnlock()

	if c == nil {
		priCacheInitOnce.Do(func() {
			priCacheMut.Lock()
			defer priCacheMut.Unlock()
			priCache = &resultCache{
				version: ver.Inc(),
				values:  typeutil.ConcurrentMap[string, bool]{},
			}
		})
		priCacheMut.RLock()
		defer priCacheMut.RUnlock()
		c = priCache
	}

	return c
}

func CleanPrivilegeCache() {
	priCacheMut.Lock()
	defer priCacheMut.Unlock()
	priCache = &resultCache{
		version: ver.Inc(),
		values:  typeutil.ConcurrentMap[string, bool]{},
	}
}

func GetResultCache(roleName, object, objectPrivilege string) (isPermit, cached bool, version int64) {
	key := fmt.Sprintf("%s_%s_%s", roleName, object, objectPrivilege)
	c := getPriCache()
	isPermit, cached = c.values.Get(key)
	return isPermit, cached, c.version
}

func SetResultCache(roleName, object, objectPrivilege string, isPermit bool, version int64) {
	key := fmt.Sprintf("%s_%s_%s", roleName, object, objectPrivilege)
	c := getPriCache()
	if c.version == version {
		c.values.Insert(key, isPermit)
	}
}

// PrivilegeCache is a cache for privilege enforce result
// version provides version control when any policy updates
type resultCache struct {
	values  typeutil.ConcurrentMap[string, bool]
	version int64
}
