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

package shardclient

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// This file is the shard client's seam for the query routing scope. It
// declares WHERE the resource group a request must be routed to enters the
// load balancer. With no provider installed every function below answers the
// empty scope, which is the absence of a filter, and routing is exactly what
// it has always been.

// routingResourceGroup returns the resource group the request on ctx must be
// routed to, or "" when it is not scoped to one and every replica of the
// collection may serve it.
//
// The scope is read off the context rather than consulted from the extension
// here. It was decided once, at the entry of the request, by
// ProxyExtension.EnsureQueryReady, and milvus bound it with
// WithQueryResourceGroup; asking the extension again this far down would let
// the answer change between the readiness check and the routing it exists to
// constrain.
//
// The nil comparison comes first so that a stock binary pays an atomic load
// and nothing else: only a request that went through the gate can carry a
// scope, and the gate only runs with a provider installed, so with none the
// context walk would be a guaranteed miss on every search and every query.
func routingResourceGroup(ctx context.Context) string {
	if extension.Caps().ProxyExt == nil {
		return ""
	}
	return extension.QueryResourceGroupFromContext(ctx)
}

// scopedCollectionWorkload stamps the request's routing scope onto a
// collection workload on its way into the load balancer.
//
// It is applied at the load balancer's OWN entry points rather than at the
// call sites that build the workload, and that is the point: the scope is a
// property of the request, every entry point already holds the request's
// context, and a construction site that forgot to carry it would not fail -
// it would route that subset of requests to another resource group's leader,
// silently. Stamping it here makes forgetting impossible instead of
// documented.
//
// A workload that already carries a scope keeps it, so a caller that has a
// reason to name one explicitly still wins.
func scopedCollectionWorkload(ctx context.Context, workload CollectionWorkLoad) CollectionWorkLoad {
	if workload.ResourceGroup == "" {
		workload.ResourceGroup = routingResourceGroup(ctx)
	}
	return workload
}

// scopedChannelWorkload is scopedCollectionWorkload for the single-channel
// entry point, which the namespace fast paths in task_search, task_query and
// task_delete reach directly without going through the collection-wide
// fan-out.
func scopedChannelWorkload(ctx context.Context, workload ChannelWorkload) ChannelWorkload {
	if workload.ResourceGroup == "" {
		workload.ResourceGroup = routingResourceGroup(ctx)
	}
	return workload
}
