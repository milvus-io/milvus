// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package featureusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestComputeLoadedEntries(t *testing.T) {
	t.Run("nothing loaded still emits the group with zeros", func(t *testing.T) {
		got := index(ComputeLoadedEntries(nil))
		require.Contains(t, got, LoadedCollections)
		assert.EqualValues(t, 0, got[LoadedCollections].Value)
		assert.EqualValues(t, 0, got[LoadedFieldsSubset].Value)
		assert.EqualValues(t, 0, got[LoadedCustomResourceGroups].Value)
		for name, e := range got {
			assert.Equal(t, GroupLoaded, e.Group, name)
		}
	})

	t.Run("counts collections, partial loads, custom resource groups and replica buckets", func(t *testing.T) {
		cols := []LoadedCollection{
			{ReplicaNumber: 1, ResourceGroups: []string{common.DefaultResourceGroupName}},
			{ReplicaNumber: 2, LoadFieldsSubset: true, ResourceGroups: []string{common.DefaultResourceGroupName, "rg_a"}},
			{ReplicaNumber: 0, ResourceGroups: []string{"rg_b"}}, // replica 0 is reported as 1
			{ReplicaNumber: 8},
		}
		entries := ComputeLoadedEntries(cols)
		got := index(entries)
		assert.EqualValues(t, 4, got[LoadedCollections].Value)
		assert.EqualValues(t, 1, got[LoadedFieldsSubset].Value)
		assert.EqualValues(t, 2, got[LoadedCustomResourceGroups].Value, "one per collection, not per replica")

		buckets := map[string]int64{}
		for _, e := range entries {
			if e.Group == GroupDist && e.Name == DistLoadedReplicaNumber {
				buckets[e.Bucket] = e.Value
			}
		}
		var total int64
		for _, v := range buckets {
			total += v
		}
		assert.EqualValues(t, 4, total, "every loaded collection falls into exactly one bucket")
		assert.EqualValues(t, 2, buckets[replicaNumberBuckets.bucket(1)], "replica 0 folds into the 1 bucket")
		assert.EqualValues(t, 1, buckets[replicaNumberBuckets.bucket(2)])
		assert.EqualValues(t, 1, buckets[replicaNumberBuckets.bucket(8)])

		// Resource group names never leak into the report.
		for _, e := range entries {
			assert.NotContains(t, e.Name, "rg_")
			assert.NotContains(t, e.Bucket, "rg_")
		}
	})
}

func TestBoolConfigEntry(t *testing.T) {
	on := BoolConfigEntry("queryNode.enableDisk", true)
	assert.Equal(t, GroupConfig, on.Group)
	assert.Equal(t, "queryNode.enableDisk=true", on.Name)
	assert.EqualValues(t, 1, on.Value)
	assert.Empty(t, on.Bucket)
	assert.Zero(t, on.LastUsedAt)

	off := BoolConfigEntry("queryNode.enableDisk", false)
	assert.Equal(t, "queryNode.enableDisk=false", off.Name)
}
