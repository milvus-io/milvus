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

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// UpdateLoadFieldsForSchema applies the original ALTER callback to local load
// metadata. It deliberately does not load a released collection or schedule a
// second broadcast while the ALTER's collection lock is held.
func (s *Server) UpdateLoadFieldsForSchema(ctx context.Context, collectionID int64, previous, target *schemapb.CollectionSchema) error {
	// meta is assigned before recovery finishes. Only Healthy publishes that
	// recovery is complete, so an empty map can safely mean not loaded.
	if err := merr.CheckHealthy(s.State()); err != nil {
		return err
	}
	if s.meta == nil {
		return merr.WrapErrServiceNotReadyMsg("querycoord load metadata is not initialized")
	}
	return s.meta.UpdateLoadFieldsForSchema(ctx, collectionID, previous, target)
}
