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

package proxy

import (
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
)

type idCache struct {
	cache *cache.Cache
}

func newIDCache(defaultExpiration, cleanupInterval time.Duration) *idCache {
	c := cache.New(defaultExpiration, cleanupInterval)
	return &idCache{
		cache: c,
	}
}

func (r *idCache) Set(id UniqueID, value bool) {
	r.cache.Set(strconv.FormatInt(id, 36), value, 0)
}

func (r *idCache) Get(id UniqueID) (value bool, exists bool) {
	valueRaw, exists := r.cache.Get(strconv.FormatInt(id, 36))
	if valueRaw == nil {
		return false, exists
	}
	return valueRaw.(bool), exists
}
