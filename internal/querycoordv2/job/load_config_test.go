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

package job

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestCheckIfLoadPartitionsExecutable(t *testing.T) {
	// helper to build a minimal Collection with the given UserSpecifiedReplicaMode
	makeCollection := func(userSpecified bool) *meta.Collection {
		return &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				UserSpecifiedReplicaMode: userSpecified,
			},
		}
	}

	// helper to build a replica map with n replicas in the default resource group
	makeReplicas := func(n int) map[int64]*meta.Replica {
		replicas := make(map[int64]*meta.Replica, n)
		for i := 0; i < n; i++ {
			id := int64(i + 1)
			replicas[id] = meta.NewReplica(&querypb.Replica{
				ID:            id,
				ResourceGroup: "__default_resource_group",
			})
		}
		return replicas
	}

	t.Run("no collection loaded — always passes", func(t *testing.T) {
		req := &AlterLoadConfigRequest{
			Current: CurrentLoadConfig{Collection: nil},
		}
		assert.NoError(t, req.CheckIfLoadPartitionsExecutable())
	})

	t.Run("replica count matches expected — passes", func(t *testing.T) {
		req := &AlterLoadConfigRequest{
			Current: CurrentLoadConfig{
				Collection: makeCollection(false),
				Replicas:   makeReplicas(2),
			},
			Expected: ExpectedLoadConfig{
				ExpectedReplicaNumber: map[string]int{"__default_resource_group": 2},
			},
		}
		assert.NoError(t, req.CheckIfLoadPartitionsExecutable())
	})

	t.Run("replica count mismatch without user-specified mode — returns error", func(t *testing.T) {
		req := &AlterLoadConfigRequest{
			Current: CurrentLoadConfig{
				Collection: makeCollection(false),
				Replicas:   makeReplicas(1), // actual: 1
			},
			Expected: ExpectedLoadConfig{
				ExpectedReplicaNumber: map[string]int{"__default_resource_group": 2}, // expected: 2
			},
		}
		assert.Error(t, req.CheckIfLoadPartitionsExecutable())
	})

	t.Run("replica count mismatch with user-specified mode — skips check, no error", func(t *testing.T) {
		// This is the exact scenario from issue #50804:
		// collection was loaded with explicit replica=1 before cluster-level config set replica=2.
		// The compliance checker must NOT flag this as a mismatch.
		req := &AlterLoadConfigRequest{
			Current: CurrentLoadConfig{
				Collection: makeCollection(true), // user explicitly set replica count
				Replicas:   makeReplicas(1),      // actual: 1
			},
			Expected: ExpectedLoadConfig{
				ExpectedReplicaNumber: map[string]int{"__default_resource_group": 2}, // cluster wants 2
			},
		}
		assert.NoError(t, req.CheckIfLoadPartitionsExecutable())
	})
}
