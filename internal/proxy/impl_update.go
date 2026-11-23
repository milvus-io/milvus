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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/dml"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Update accepts one literal assignment set for a list of primary keys.
func (node *Proxy) Update(ctx context.Context, request *milvuspb.UpdateRequest) (*milvuspb.MutationResult, error) {
	if !paramtable.Get().ProxyCfg.PKUpdateEnabled.GetAsBool() {
		return failedMutationResult(merr.Wrap(merr.ErrServiceUnimplemented, "PK-update admission is disabled (proxy.pkUpdate.enabled=false)")), nil
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return failedMutationResult(err), nil
	}
	if err := checkExternalCollectionBlockedForWrite(ctx, node.GetMetaCache(), request.GetDbName(), request.GetCollectionName(), "update"); err != nil {
		return failedMutationResult(err), nil
	}
	result, err := dml.Update(ctx, node.GetMetaCache(), request)
	if err != nil {
		return failedMutationResult(err), nil
	}
	return result, nil
}
