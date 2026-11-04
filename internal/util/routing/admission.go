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
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NamespaceShardBy is the shard_by expression of a collection routed by
// namespace.
const NamespaceShardBy = "hash(" + common.NamespaceFieldName + ")"

// CheckShardByAdmission refuses a routing post-image whose shard_by the
// collection's rows were never placed by (design §3.1).
//
// The namespace routing key is valid only for a collection whose rows have
// ALWAYS been placed by it, and that is one configuration, not every namespace
// collection: the proxy places by namespace only when
// namespace.sharding.enabled=true AND namespace.mode=partition_key, and
// sharding.enabled is written as false at create time unless the request set
// it. A default namespace collection therefore has every row spread over all
// shards by primary key, and routing hash($namespace_id) onto it would send a
// namespace's NEW rows to one shard while its existing rows stay everywhere -- a
// delete routed by the namespace hash then reaches one shard and silently misses
// the rest. Both properties are immutable after creation, so placement history
// is decidable from the properties alone.
//
// It is the ONE implementation shared by the two sides of the fence: the split
// builder runs it before the broadcast, on the properties the genesis schema
// carries, and rootcoord runs it again when a post-image is applied, on the
// collection meta. A second copy would let them drift, and a refusal only the
// apply side makes wedges a collection whose source is already fenced.
//
// Every refusal is a System error: the post-image is planned by the split
// coordinator, never typed by a user, so a bad one is a Milvus bug. A malformed
// property is relabeled to System for the same reason.
func CheckShardByAdmission(shardBy string, properties []*commonpb.KeyValuePair) error {
	if shardBy != NamespaceShardBy {
		return nil
	}
	enabled, err := common.IsNamespaceShardingEnabled(properties...)
	if err != nil {
		// A deliberate relabel, not added context: the inner error is
		// ParameterInvalid, an InputError, because the property parser serves
		// user requests too. Here the property is collection meta a coordinator
		// planned against, so the refusal is System; merr.Wrap would keep the
		// Input class and blame a user for a planning bug.
		return merr.WrapErrServiceInternalErr(err, "routing by %s needs a well-formed %s", NamespaceShardBy, common.NamespaceShardingEnabledKey)
	}
	if !enabled || !common.IsNamespaceModePartitionKey(properties...) {
		return merr.WrapErrServiceInternalMsg(
			"cannot route by %s: its rows are placed by primary key "+
				"(namespace.sharding.enabled=%t, namespace.mode=%s), so it must split under hash(pk) or not at all",
			NamespaceShardBy, enabled, common.GetNamespaceMode(properties...))
	}
	return nil
}

// CheckNamespaceRelabelGranularity refuses a namespace-routed post-image whose
// modulus does not divide the collection's partition-key bucket count.
//
// A namespace split moves data only by relabeling whole segments, and in
// partition_key mode a segment holds exactly one partition-key bucket: a row's
// bucket is HashString2Uint32(namespace) % buckets (typeutil.HashKey2Partitions
// over every partition name) and its residue is the same hash % modulus. A
// bucket therefore lies wholly on one residue -- namely bucket % modulus -- if
// and only if the modulus divides the bucket count; any other modulus cuts a
// bucket's segments across two shards, which no relabel can place. buckets is
// the collection's partition count, fixed at creation in partition_key mode.
//
// A shard_by other than the namespace key is not constrained: hash(pk) splits
// rewrite rather than relabel. System errors, for the same reason as
// CheckShardByAdmission.
func CheckNamespaceRelabelGranularity(shardBy string, modulus uint64, buckets int) error {
	if shardBy != NamespaceShardBy {
		return nil
	}
	if modulus == 0 || buckets <= 0 || uint64(buckets)%modulus != 0 {
		return merr.WrapErrServiceInternalMsg(
			"cannot route by %s at modulus %d: it must divide the %d partition-key buckets, "+
				"or a bucket's segments land on two shards and cannot be relabeled",
			NamespaceShardBy, modulus, buckets)
	}
	return nil
}

// CheckAdmissionPropertiesAgree refuses a SplitShard message whose genesis
// schema properties disagree with the collection meta's on what namespace
// admission reads.
//
// The two sides of the fence read admission from two copies of the same
// immutable facts: SplitShardParam.Validate and the SplitShard ack callback read
// the genesis schema's properties, and every routing apply reads the meta's. If
// the copies disagree, a post-image admitted before the fence can be refused
// after it, with the source already fenced. The disagreement is therefore
// refused itself, before the broadcast by the planner's check and loudly in the
// ack callback, rather than surfacing later as a refusal nobody can explain.
//
// Only what admission reads is compared, the way it reads it:
// namespace.sharding.enabled (absent reads false; a malformed value is its own
// state) and namespace.mode. System error, for the same reason as
// CheckShardByAdmission.
func CheckAdmissionPropertiesAgree(genesis, meta []*commonpb.KeyValuePair) error {
	genesisEnabled, genesisErr := common.IsNamespaceShardingEnabled(genesis...)
	metaEnabled, metaErr := common.IsNamespaceShardingEnabled(meta...)
	genesisMode, metaMode := common.GetNamespaceMode(genesis...), common.GetNamespaceMode(meta...)
	if (genesisErr == nil) == (metaErr == nil) && genesisEnabled == metaEnabled && genesisMode == metaMode {
		return nil
	}
	return merr.WrapErrServiceInternalMsg(
		"the split shard message's collection properties disagree with the collection meta on namespace admission: "+
			"genesis %s=%s %s=%s, meta %s=%s %s=%s",
		common.NamespaceShardingEnabledKey, admissionPropertyValue(genesis, common.NamespaceShardingEnabledKey),
		common.NamespaceModeKey, genesisMode,
		common.NamespaceShardingEnabledKey, admissionPropertyValue(meta, common.NamespaceShardingEnabledKey),
		common.NamespaceModeKey, metaMode)
}

// admissionPropertyValue is the raw value of key, or "<unset>".
func admissionPropertyValue(properties []*commonpb.KeyValuePair, key string) string {
	for _, kv := range properties {
		if kv.GetKey() == key {
			return kv.GetValue()
		}
	}
	return "<unset>"
}
