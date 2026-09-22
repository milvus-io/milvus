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

package extension

import "context"

type queryResourceGroupKey struct{}

// WithQueryResourceGroup pins the query running under ctx to one resource
// group: the proxy routes it to the shard leaders of that group only and
// attributes its latency to that group. A request hook sets it from Before;
// nothing in a stock binary does.
func WithQueryResourceGroup(ctx context.Context, resourceGroup string) context.Context {
	return context.WithValue(ctx, queryResourceGroupKey{}, resourceGroup)
}

// QueryResourceGroupFromContext returns the resource group WithQueryResourceGroup
// pinned, or "" when the query is not pinned.
func QueryResourceGroupFromContext(ctx context.Context) string {
	resourceGroup, _ := ctx.Value(queryResourceGroupKey{}).(string)
	return resourceGroup
}
