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

package planparserv2

import (
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// Capture static configuration on first use, after paramtable initialization.
// No configuration lookup or tree-size estimation is needed on subsequent parses.
var exprCache = sync.OnceValue(func() *expressionCache {
	return newExpressionCache(paramtable.Get())
})

type expressionCache struct {
	cache *expirable.LRU[string, any]
}

func newExpressionCache(params *paramtable.ComponentParam) *expressionCache {
	cfg := &params.ProxyCfg
	c := &expressionCache{}
	if cfg.ExprParseCacheEnabled.GetAsBool() {
		c.cache = expirable.NewLRU[string, any](cfg.ExprParseCacheMaxEntries.GetAsInt(), nil, 10*time.Minute)
	}
	return c
}
