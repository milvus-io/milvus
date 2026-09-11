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

package featureusage

import (
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Names of GroupDeclared predicates. This is the one hand-maintained list of
// the static side; everything else is an enum walk, an open-value count or
// an open-key count.
const (
	DeclaredPartitionKey       = "is_partition_key"
	DeclaredClusteringKey      = "is_clustering_key"
	DeclaredEnableDynamicField = "enable_dynamic_field"
	DeclaredEnableNamespace    = "enable_namespace"
	DeclaredNullable           = "nullable"
	DeclaredDefaultValue       = "default_value"
	DeclaredAutoID             = "auto_id"
	DeclaredMultiVectorField   = "multi_vector_field"
	DeclaredStructArrayFields  = "struct_array_fields"
	DeclaredIsAutoIndex        = "is_auto_index"
	declaredConsistencyPrefix  = "consistency_level="
)

// Names of GroupObjects entries.
const (
	ObjectDatabases       = "databases"
	ObjectAliases         = "aliases"
	ObjectCustomRoles     = "custom_roles"
	ObjectGrants          = "grants"
	ObjectPrivilegeGroups = "privilege_groups"
)

// Recognized provider names. Provider strings are validated when a function is
// created, but the report folds anything outside these sets into OtherValue so
// that the sanitization rule does not depend on that validation.
var (
	embeddingProviders = set(
		"openai", "azure_openai", "dashscope", "bedrock", "vertexai", "voyageai",
		"cohere", "siliconflow", "tei", "zilliz", "gemini", "huggingface", "yc",
	)
	rerankProviders = set(
		"ali", "cohere", "huggingface", "siliconflow", "tei", "vllm", "voyageai", "zilliz",
	)
)

// serverManagedKeys are collection properties the server writes on every
// collection and users cannot set or alter; reporting them would count every
// collection and carry no signal.
var serverManagedKeys = set(common.MaxFieldIDKey)

const (
	functionParamProvider = "provider"
	functionParamReranker = "reranker"
	// rerankModelFunction is the rerank function name that carries a provider.
	rerankModelFunction = "model"
)

// CollectionInput is everything the rootcoord side contributes.
type CollectionInput struct {
	Databases   []*model.Database
	Collections []*model.Collection

	// Object counts. The report carries counts only, never names.
	AliasCount          int
	CustomRoleCount     int // roles other than the built-in admin / public
	GrantCount          int
	PrivilegeGroupCount int
}

// ComputeCollectionEntries computes the static groups that derive from
// collection and database metadata: field_types, functions, providers,
// declared, properties, db_properties, field_params, objects, dist.
// It reads only the metadata passed in and holds no state across calls.
func ComputeCollectionEntries(in CollectionInput) []*internalpb.FeatureEntry {
	c := newCollector()

	// Enum walks emit every value, so a zero is "exists in this build, unused"
	// and an absent value is "does not exist in this build".
	for _, v := range sortedEnumValues(schemapb.DataType_name) {
		if v == int32(schemapb.DataType_None) {
			continue
		}
		c.ensure(GroupFieldTypes, schemapb.DataType_name[v], "")
	}
	for _, v := range sortedEnumValues(schemapb.FunctionType_name) {
		if v == int32(schemapb.FunctionType_Unknown) {
			continue
		}
		c.ensure(GroupFunctions, schemapb.FunctionType_name[v], "")
	}
	for _, name := range []string{
		DeclaredPartitionKey, DeclaredClusteringKey, DeclaredEnableDynamicField,
		DeclaredEnableNamespace, DeclaredNullable, DeclaredDefaultValue, DeclaredAutoID,
		DeclaredMultiVectorField, DeclaredStructArrayFields,
	} {
		c.ensure(GroupDeclared, name, "")
	}

	for _, col := range in.Collections {
		computeOneCollection(c, col)
	}

	nonDefaultDBs := 0
	for _, db := range in.Databases {
		if db.Name != util.DefaultDBName {
			nonDefaultDBs++
		}
		seen := newSeen()
		for _, kv := range db.Properties {
			c.addOnce(seen, GroupDBProperties, propertyEntryName(kv), "")
		}
	}

	c.set(GroupObjects, ObjectDatabases, int64(nonDefaultDBs))
	c.set(GroupObjects, ObjectAliases, int64(in.AliasCount))
	c.set(GroupObjects, ObjectCustomRoles, int64(in.CustomRoleCount))
	c.set(GroupObjects, ObjectGrants, int64(in.GrantCount))
	c.set(GroupObjects, ObjectPrivilegeGroups, int64(in.PrivilegeGroupCount))

	return c.entries()
}

func computeOneCollection(c *collector, col *model.Collection) {
	seen := newSeen()

	fields := allFields(col)
	vectorFields := 0
	var maxDim, maxLength, maxCapacity int64 = -1, -1, -1
	hasNullable, hasDefault, hasAutoID := false, false, col.AutoID
	hasPartitionKey, hasClusteringKey := false, false

	for _, f := range fields {
		if f.IsDynamic {
			// The internal $meta field; its existence is already reported by
			// enable_dynamic_field and its JSON type would inflate field_types.
			continue
		}
		c.addOnce(seen, GroupFieldTypes, f.DataType.String(), "")
		if typeutil.IsVectorType(f.DataType) {
			vectorFields++
			if d, ok := int64Param(f.TypeParams, common.DimKey); ok && d > maxDim {
				maxDim = d
			}
		}
		if l, ok := int64Param(f.TypeParams, common.MaxLengthKey); ok && l > maxLength {
			maxLength = l
		}
		if cap, ok := int64Param(f.TypeParams, common.MaxCapacityKey); ok && cap > maxCapacity {
			maxCapacity = cap
		}
		hasNullable = hasNullable || f.Nullable
		hasDefault = hasDefault || f.DefaultValue != nil
		hasAutoID = hasAutoID || (f.IsPrimaryKey && f.AutoID)
		hasPartitionKey = hasPartitionKey || f.IsPartitionKey
		hasClusteringKey = hasClusteringKey || f.IsClusteringKey
		for _, kv := range f.TypeParams {
			c.addOnce(seen, GroupFieldParams, propertyEntryName(kv), "")
		}
	}

	for _, fn := range col.Functions {
		c.addOnce(seen, GroupFunctions, fn.Type.String(), "")
		switch fn.Type {
		case schemapb.FunctionType_TextEmbedding:
			if p, ok := stringParam(fn.Params, functionParamProvider); ok {
				c.addOnce(seen, GroupProviders, foldValue(strings.ToLower(p), embeddingProviders), "")
			}
		case schemapb.FunctionType_Rerank:
			if r, ok := stringParam(fn.Params, functionParamReranker); ok && strings.ToLower(r) == rerankModelFunction {
				if p, ok := stringParam(fn.Params, functionParamProvider); ok {
					c.addOnce(seen, GroupProviders, RerankProviderPrefix+foldValue(strings.ToLower(p), rerankProviders), "")
				}
			}
		}
	}

	if hasPartitionKey {
		c.addOnce(seen, GroupDeclared, DeclaredPartitionKey, "")
	}
	if hasClusteringKey {
		c.addOnce(seen, GroupDeclared, DeclaredClusteringKey, "")
	}
	if col.EnableDynamicField || boolProperty(col.Properties, common.EnableDynamicSchemaKey) {
		c.addOnce(seen, GroupDeclared, DeclaredEnableDynamicField, "")
	}
	if col.EnableNamespace || boolProperty(col.Properties, common.NamespaceShardingEnabledKey) {
		c.addOnce(seen, GroupDeclared, DeclaredEnableNamespace, "")
	}
	if hasNullable {
		c.addOnce(seen, GroupDeclared, DeclaredNullable, "")
	}
	if hasDefault {
		c.addOnce(seen, GroupDeclared, DeclaredDefaultValue, "")
	}
	if hasAutoID {
		c.addOnce(seen, GroupDeclared, DeclaredAutoID, "")
	}
	if vectorFields > 1 {
		c.addOnce(seen, GroupDeclared, DeclaredMultiVectorField, "")
	}
	if len(col.StructArrayFields) > 0 {
		c.addOnce(seen, GroupDeclared, DeclaredStructArrayFields, "")
	}
	if name, ok := commonpb.ConsistencyLevel_name[int32(col.ConsistencyLevel)]; ok {
		c.addOnce(seen, GroupDeclared, declaredConsistencyPrefix+name, "")
	}

	for _, kv := range col.Properties {
		if _, managed := serverManagedKeys[kv.GetKey()]; managed {
			continue
		}
		c.addOnce(seen, GroupProperties, propertyEntryName(kv), "")
	}

	c.add(GroupDist, DistPartitionCount, partitionCountBuckets.bucket(int64(len(col.Partitions))))
	c.add(GroupDist, DistShardsNum, shardsNumBuckets.bucket(int64(col.ShardsNum)))
	if maxDim >= 0 {
		c.add(GroupDist, DistDim, dimBuckets.bucket(maxDim))
	}
	if maxLength >= 0 {
		c.add(GroupDist, DistMaxLength, maxLengthBuckets.bucket(maxLength))
	}
	if maxCapacity >= 0 {
		c.add(GroupDist, DistMaxCapacity, maxCapacityBuckets.bucket(maxCapacity))
	}
	replicas := int64(1)
	if r, ok := int64Param(col.Properties, common.CollectionReplicaNumber); ok && r > 0 {
		replicas = r
	}
	c.add(GroupDist, DistReplicaNumber, replicaNumberBuckets.bucket(replicas))
}

// allFields returns the top-level fields plus the fields nested in struct
// array fields, which are where ArrayOfVector and friends live.
func allFields(col *model.Collection) []*model.Field {
	fields := make([]*model.Field, 0, len(col.Fields))
	fields = append(fields, col.Fields...)
	for _, s := range col.StructArrayFields {
		fields = append(fields, s.Fields...)
	}
	return fields
}

// propertyEntryName is the entry name for one key/value pair of a property,
// type-param or index-param list. Official keys are named; boolean values are
// split into key=true / key=false because "who turned it off" is the question
// a deprecation decision asks; any other value is dropped; non-official keys
// fold into CustomKey. This is the only place a key from metadata becomes an
// output string.
func propertyEntryName(kv *commonpb.KeyValuePair) string {
	return keyValueEntryName(kv.GetKey(), kv.GetValue())
}

func keyValueEntryName(key, value string) string {
	if !common.IsOfficialFeatureKey(key) {
		return CustomKey
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return key + "=true"
	case "false":
		return key + "=false"
	}
	return key
}

func foldValue(v string, recognized map[string]struct{}) string {
	if _, ok := recognized[v]; ok {
		return v
	}
	return OtherValue
}

func boolProperty(kvs []*commonpb.KeyValuePair, key string) bool {
	v, ok := stringParam(kvs, key)
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
}

func stringParam(kvs []*commonpb.KeyValuePair, key string) (string, bool) {
	for _, kv := range kvs {
		if kv.GetKey() == key {
			return kv.GetValue(), true
		}
	}
	return "", false
}

func int64Param(kvs []*commonpb.KeyValuePair, key string) (int64, bool) {
	v, ok := stringParam(kvs, key)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func sortedEnumValues(names map[int32]string) []int32 {
	vals := make([]int32, 0, len(names))
	for v := range names {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals
}

func set(items ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[it] = struct{}{}
	}
	return m
}

// collector accumulates (group, name, bucket) -> value and emits a
// deterministic, sorted entry list.
type collector struct {
	values map[entryKey]int64
}

type entryKey struct {
	group, name, bucket string
}

func newCollector() *collector {
	return &collector{values: make(map[entryKey]int64)}
}

// seen tracks which (group, name) pairs one collection has already been
// counted for, so a collection contributes at most one to each entry.
type seen map[entryKey]struct{}

func newSeen() seen { return make(seen) }

func (c *collector) ensure(group, name, bucket string) {
	k := entryKey{group, name, bucket}
	if _, ok := c.values[k]; !ok {
		c.values[k] = 0
	}
}

func (c *collector) add(group, name, bucket string) {
	c.values[entryKey{group, name, bucket}]++
}

func (c *collector) set(group, name string, v int64) {
	c.values[entryKey{group, name, ""}] = v
}

func (c *collector) addOnce(s seen, group, name, bucket string) {
	k := entryKey{group, name, bucket}
	if _, dup := s[k]; dup {
		return
	}
	s[k] = struct{}{}
	c.values[k]++
}

func (c *collector) entries() []*internalpb.FeatureEntry {
	keys := make([]entryKey, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].group != keys[j].group {
			return keys[i].group < keys[j].group
		}
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].bucket < keys[j].bucket
	})
	out := make([]*internalpb.FeatureEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, &internalpb.FeatureEntry{
			Group:  k.group,
			Name:   k.name,
			Value:  c.values[k],
			Bucket: k.bucket,
		})
	}
	return out
}
