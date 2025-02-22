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
	"context"
	"fmt"
	"strings"

	"github.com/casbin/casbin/v2/model"
	jsonadapter "github.com/casbin/json-adapter/v2"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// MetaCacheCasbinAdapter is the implementation of `persist.Adapter` with Cache
// Since the usage shall be read-only, it implements only `LoadPolicy` for now.
type MetaCacheCasbinAdapter struct {
	cacheSource func() Cache
}

func NewMetaCacheCasbinAdapter(cacheSource func() Cache) *MetaCacheCasbinAdapter {
	return &MetaCacheCasbinAdapter{
		cacheSource: cacheSource,
	}
}

// LoadPolicy loads all policy rules from the storage.
// Implementing `persist.Adapter`.
func (a *MetaCacheCasbinAdapter) LoadPolicy(model model.Model) error {
	cache := a.cacheSource()
	if cache == nil {
		return merr.WrapErrServiceInternal("cache source return nil cache")
	}
	policyInfo := strings.Join(cache.GetPrivilegeInfo(context.Background()), ",")

	policy := fmt.Sprintf("[%s]", policyInfo)
	byteSource := []byte(policy)
	jAdapter := jsonadapter.NewAdapter(&byteSource)
	return jAdapter.LoadPolicy(model)
}

// SavePolicy saves all policy rules to the storage.
// Implementing `persist.Adapter`.
// MetaCacheCasbinAdapter is read-only, always returns error
func (a *MetaCacheCasbinAdapter) SavePolicy(model model.Model) error {
	return merr.WrapErrServiceInternal("MetaCacheCasbinAdapter is read-only, but received SavePolicy call")
}

// AddPolicy adds a policy rule to the storage.
// Implementing `persist.Adapter`.
// MetaCacheCasbinAdapter is read-only, always returns error
func (a *MetaCacheCasbinAdapter) AddPolicy(sec string, ptype string, rule []string) error {
	return merr.WrapErrServiceInternal("MetaCacheCasbinAdapter is read-only, but received AddPolicy call")
}

// RemovePolicy removes a policy rule from the storage.
// Implementing `persist.Adapter`.
// MetaCacheCasbinAdapter is read-only, always returns error
func (a *MetaCacheCasbinAdapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return merr.WrapErrServiceInternal("MetaCacheCasbinAdapter is read-only, but received RemovePolicy call")
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
// This is part of the Auto-Save feature.
func (a *MetaCacheCasbinAdapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return merr.WrapErrServiceInternal("MetaCacheCasbinAdapter is read-only, but received RemoveFilteredPolicy call")
}
