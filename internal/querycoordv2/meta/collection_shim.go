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

import coordmeta "github.com/milvus-io/milvus/pkg/v3/coordmeta/querycoord"

// CollectionManager and its domain types (Collection / Partition /
// CollectionOperator) now live in the shared pkg/v3/coordmeta/querycoord package
// so the pooled catalog service can reuse the same implementation. These aliases
// keep the existing internal/querycoordv2 call sites compiling unchanged.
//
// The CoordinatorBroker defined in this package satisfies the minimal
// coordmeta.Broker that CollectionManager.Recover expects, so passing s.broker
// to Recover keeps working without a wrapper.
type (
	Collection         = coordmeta.Collection
	Partition          = coordmeta.Partition
	CollectionManager  = coordmeta.CollectionManager
	CollectionOperator = coordmeta.CollectionOperator
)

var (
	NewCollectionManager    = coordmeta.NewCollectionManager
	SetNotifierCollectionOp = coordmeta.SetNotifierCollectionOp
)
