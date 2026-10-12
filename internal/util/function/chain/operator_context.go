// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Derive every operator's scope from the execution's parent, rather than the
// previous operator. Cached chains remain immutable and log tags cannot leak
// between steps or concurrent requests. flat_map's internal map/explode share
// this outer scope, because both implement one public operator.
func (fc *FuncChain) operatorContext(parent *types.FuncContext, index int, op Operator) *types.FuncContext {
	name := fc.name
	if name == "" {
		name = fc.stage
	}
	scope := parent.RequestContext()
	scope.ResourceName = fmt.Sprintf("%s/step/%d/%s", name, index, op.Name())
	ctx := types.WithRequestContext(parent.Context(), scope)
	fields := []mlog.Field{mlog.String("resource_name", scope.ResourceName)}
	if scope.Database != "" {
		fields = append(fields, mlog.FieldDbName(scope.Database))
	}
	if scope.Collection != "" {
		fields = append(fields, mlog.FieldCollectionName(scope.Collection))
	}
	if scope.User != "" {
		fields = append(fields, mlog.String("user", scope.User))
	}
	return parent.WithContext(mlog.WithFields(ctx, fields...))
}
