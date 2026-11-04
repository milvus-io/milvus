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

package routing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func kv(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func TestCheckShardByAdmission(t *testing.T) {
	// A primary-key routed post-image is admitted whatever the properties say.
	require.NoError(t, CheckShardByAdmission("hash(pk)", nil))
	require.NoError(t, CheckShardByAdmission("", []*commonpb.KeyValuePair{kv(common.NamespaceShardingEnabledKey, "false")}))

	// The namespace key: sharding enabled in partition_key mode, explicit or by
	// default mode.
	require.NoError(t, CheckShardByAdmission(NamespaceShardBy, []*commonpb.KeyValuePair{
		kv(common.NamespaceShardingEnabledKey, "true"),
		kv(common.NamespaceModeKey, common.NamespaceModePartitionKey),
	}))
	require.NoError(t, CheckShardByAdmission(NamespaceShardBy, []*commonpb.KeyValuePair{
		kv(common.NamespaceShardingEnabledKey, "true"),
	}))

	for _, tc := range []struct {
		name       string
		properties []*commonpb.KeyValuePair
		contains   string
	}{
		{name: "no properties", contains: "placed by primary key"},
		{name: "sharding disabled", properties: []*commonpb.KeyValuePair{kv(common.NamespaceShardingEnabledKey, "false")}, contains: "placed by primary key"},
		{
			name: "partition mode",
			properties: []*commonpb.KeyValuePair{
				kv(common.NamespaceShardingEnabledKey, "true"),
				kv(common.NamespaceModeKey, common.NamespaceModePartition),
			},
			contains: "placed by primary key",
		},
		{name: "malformed sharding property", properties: []*commonpb.KeyValuePair{kv(common.NamespaceShardingEnabledKey, "yes")}, contains: common.NamespaceShardingEnabledKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckShardByAdmission(NamespaceShardBy, tc.properties)
			// System, never Input: the post-image is planned by a coordinator. The
			// malformed property's ParameterInvalid is relabeled.
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))
			assert.NotEqual(t, merr.InputError, merr.GetErrorType(err))
			assert.ErrorContains(t, err, tc.contains)
		})
	}
}

func TestCheckNamespaceRelabelGranularity(t *testing.T) {
	for _, modulus := range []uint64{1, 2, 4, 8, 16} {
		require.NoError(t, CheckNamespaceRelabelGranularity(NamespaceShardBy, modulus, 16), "modulus %d", modulus)
	}
	for _, modulus := range []uint64{0, 3, 5, 32} {
		err := CheckNamespaceRelabelGranularity(NamespaceShardBy, modulus, 16)
		require.ErrorIs(t, err, merr.ErrServiceInternal, "modulus %d", modulus)
		assert.ErrorContains(t, err, "must divide the 16 partition-key buckets")
	}
	require.ErrorIs(t, CheckNamespaceRelabelGranularity(NamespaceShardBy, 2, 0), merr.ErrServiceInternal)

	// Primary-key routing rewrites instead of relabeling; no bucket constraint.
	require.NoError(t, CheckNamespaceRelabelGranularity("hash(pk)", 3, 16))
	require.NoError(t, CheckNamespaceRelabelGranularity("", 3, 16))
}

// TestNamespaceBucketLiesOnOneResidueWhenTheModulusDividesTheBuckets pins the
// premise CheckNamespaceRelabelGranularity rests on: the partition-key bucket a
// namespace's rows go to (typeutil.HashKey2Partitions over every partition
// name) and the shard the proxy routes that namespace to
// (typeutil.HashNamespace2Channels) are the same hash reduced by two moduli, so
// when the modulus divides the bucket count the residue is bucket % modulus. If
// either side ever changes its hash, this fails before a relabel splits a
// bucket.
func TestNamespaceBucketLiesOnOneResidueWhenTheModulusDividesTheBuckets(t *testing.T) {
	const buckets = 16
	partitionNames := make([]string, buckets)
	for i := range partitionNames {
		partitionNames[i] = fmt.Sprintf("_default_%d", i)
	}
	namespaces := make([]string, 0, 512)
	for i := 0; i < 500; i++ {
		namespaces = append(namespaces, fmt.Sprintf("tenant-%d", i))
	}
	// A key longer than the hashed prefix must behave the same on both sides.
	long := make([]byte, 300)
	for i := range long {
		long[i] = byte('a' + i%26)
	}
	namespaces = append(namespaces, string(long), "", "ns")

	keys := &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
		Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: namespaces}},
	}}}
	bucketOf, err := typeutil.HashKey2Partitions(keys, partitionNames)
	require.NoError(t, err)
	require.Len(t, bucketOf, len(namespaces))

	for _, modulus := range []int{1, 2, 4, 8, 16} {
		shards := make([]string, modulus)
		for i, namespace := range namespaces {
			require.Equal(t, bucketOf[i]%uint32(modulus), typeutil.HashNamespace2Channels(namespace, shards),
				"namespace %q at modulus %d", namespace, modulus)
		}
	}
}

func TestCheckAdmissionPropertiesAgree(t *testing.T) {
	placed := []*commonpb.KeyValuePair{
		kv(common.NamespaceShardingEnabledKey, "true"),
		kv(common.NamespaceModeKey, common.NamespaceModePartitionKey),
	}
	// Agreement is on what admission reads, not on the raw lists.
	require.NoError(t, CheckAdmissionPropertiesAgree(nil, nil))
	require.NoError(t, CheckAdmissionPropertiesAgree(nil, []*commonpb.KeyValuePair{kv(common.NamespaceShardingEnabledKey, "false")}),
		"an absent sharding property reads false")
	require.NoError(t, CheckAdmissionPropertiesAgree(placed, append([]*commonpb.KeyValuePair{kv("collection.ttl.seconds", "10")}, placed...)),
		"a property admission does not read")

	for _, tc := range []struct {
		name          string
		genesis, meta []*commonpb.KeyValuePair
	}{
		{name: "sharding enabled on one side only", genesis: placed, meta: []*commonpb.KeyValuePair{kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)}},
		{name: "a different namespace mode", genesis: placed, meta: []*commonpb.KeyValuePair{
			kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartition),
		}},
		{name: "a malformed sharding property on one side", genesis: []*commonpb.KeyValuePair{kv(common.NamespaceShardingEnabledKey, "yes")}, meta: nil},
		{name: "the genesis carries no properties at all", genesis: nil, meta: placed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckAdmissionPropertiesAgree(tc.genesis, tc.meta)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.False(t, merr.IsRetryableErr(err))
			assert.ErrorContains(t, err, "disagree with the collection meta")
		})
	}
}
