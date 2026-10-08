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
	DeclaredStructArrayFields  = "struct_array"
	// DeclaredExternalCollection: the collection reads an external source
	// (collection.external_source); the source itself is stored outside the
	// properties and is never named.
	DeclaredExternalCollection = "external_collection"
	DeclaredIsAutoIndex        = "autoindex"
	declaredConsistencyPrefix  = "consistency_level="
)

// Names of GroupObjects entries.
const (
	ObjectDatabases = "databases"
	ObjectAliases   = "aliases"
)

// Recognized provider names. Provider strings are validated when a function is
// created, but the report folds anything outside these sets into OtherValue so
// that the sanitization rule does not depend on that validation.
var (
	embeddingProviders = set(
		"openai", "azure_openai", "dashscope", "bedrock", "vertexai", "voyageai",
		"cohere", "siliconflow", "tei", "zilliz", "gemini", "huggingface", "yc",
	)
)

// serverManagedKeys are collection properties the server writes on every
// collection without the user asking; reporting them would count every
// collection and carry no signal. max_field_id is bookkeeping; timezone is
// copied from the database (or the default) onto each new collection, and the
// database's own timezone is what db_properties reports; cipher.ezID is copied
// from an encrypted database onto each of its collections, and cipher.enabled
// on the database is the user's choice.
var serverManagedKeys = set(common.MaxFieldIDKey, common.TimezoneKey, common.EncryptionEzIDKey)

// serverDefaultProperties are key=value pairs the server writes on every new
// collection as the default; only a different value is the user's.
var serverDefaultProperties = map[string]string{
	common.NamespaceShardingEnabledKey: "false",
}

const functionParamProvider = "provider"

// CollectionInput is everything the rootcoord side contributes.
type CollectionInput struct {
	Databases   []*model.Database
	Collections []*model.Collection

	// Object counts. The report carries counts only, never names.
	AliasCount int
}

// CollectionContext is what the groups computed outside RootCoord need to
// know about the collections RootCoord currently has: QueryCoord's loaded
// group and DataCoord's segment group hold collection IDs and resolve them
// against this rather than against copies of the schema they may hold (a
// QueryCoord recovered after a restart holds none). It is taken from the same
// snapshot as the collection groups, so one report describes one set of
// collections: a collection that is being dropped is absent here and skipped
// everywhere, instead of surviving in the segment group alone.
type CollectionContext struct {
	// FieldCount is, per available collection, the number of fields a load
	// request can name: every user field plus the sub-fields of struct array
	// fields; system fields are never named.
	FieldCount map[int64]int
	// BM25 marks the collections that declare a BM25 function. Full-text
	// search has stats on the segment under storage V1/V2 but keeps them in
	// the manifest under V3, so the schema is the one place that always says.
	BM25 map[int64]bool
}

// Known reports whether the context describes collections at all. A zero
// context (no RootCoord snapshot in hand, as in a unit test of one
// coordinator) disables the filtering instead of filtering everything out.
func (cc CollectionContext) Known() bool {
	return cc.FieldCount != nil
}

// Context derives the CollectionContext from the snapshot.
func (in CollectionInput) Context() CollectionContext {
	cc := CollectionContext{
		FieldCount: make(map[int64]int, len(in.Collections)),
		BM25:       make(map[int64]bool),
	}
	for _, col := range in.Collections {
		if col == nil {
			continue
		}
		n := 0
		for _, f := range col.Fields {
			if !common.IsSystemField(f.FieldID) {
				n++
			}
		}
		for _, sf := range col.StructArrayFields {
			n += len(sf.Fields)
		}
		cc.FieldCount[col.CollectionID] = n
		for _, fn := range col.Functions {
			if fn != nil && fn.Type == schemapb.FunctionType_BM25 {
				cc.BM25[col.CollectionID] = true
			}
		}
	}
	return cc
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
		DeclaredMultiVectorField, DeclaredStructArrayFields, DeclaredExternalCollection,
	} {
		c.ensure(GroupDeclared, name, "")
	}

	dbByName := make(map[string]*model.Database, len(in.Databases))
	for _, db := range in.Databases {
		if db != nil {
			dbByName[db.Name] = db
		}
	}
	for _, col := range in.Collections {
		computeOneCollection(c, col, dbByName[col.DBName])
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

	return c.entries()
}

func computeOneCollection(c *collector, col *model.Collection, db *model.Database) {
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
		if isServerField(f) {
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
		// Rerank functions are request-level only (schema validation rejects
		// them in a collection), so their providers are request counters
		// (rerank_provider=*), not a schema trait.
		if fn.Type == schemapb.FunctionType_TextEmbedding {
			// The runtime matches the parameter key case-insensitively.
			if p, ok := stringParamFold(fn.Params, functionParamProvider); ok {
				c.addOnce(seen, GroupProviders, foldValue(strings.ToLower(p), embeddingProviders), "")
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
	// namespace.sharding.enabled is a sub-option that only acts when the
	// collection has namespaces, so the schema flag alone is the predicate.
	if col.EnableNamespace {
		c.addOnce(seen, GroupDeclared, DeclaredEnableNamespace, "")
	}
	if col.ExternalSource != "" {
		c.addOnce(seen, GroupDeclared, DeclaredExternalCollection, "")
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
		if v, ok := serverDefaultProperties[kv.GetKey()]; ok && strings.EqualFold(v, kv.GetValue()) {
			continue
		}
		// Namespaces require partition key isolation; the server sets it when
		// the namespace field is added.
		if kv.GetKey() == common.PartitionKeyIsolationKey && col.EnableNamespace {
			continue
		}
		c.addOnce(seen, GroupProperties, propertyEntryName(kv), "")
	}

	// Partitions being dropped stay in the model until the tombstone sweeper
	// removes them, as RootCoord's own partition count knows.
	c.add(GroupDist, DistPartitionCount, partitionCountBuckets.bucket(int64(col.GetPartitionNum(true))))
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
	// The declared replica number resolves like the load does: the collection
	// property, then the database's. A collection that sets neither inherits
	// the cluster default and is not bucketed here; the loaded group reports
	// what it actually runs with.
	if r, ok := int64Param(col.Properties, common.CollectionReplicaNumber); ok && r > 0 {
		c.add(GroupDist, DistReplicaNumber, replicaNumberBuckets.bucket(r))
	} else if r, ok := int64Param(dbProperties(db), common.DatabaseReplicaNumber); ok && r > 0 {
		c.add(GroupDist, DistReplicaNumber, replicaNumberBuckets.bucket(r))
	}
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

func dbProperties(db *model.Database) []*commonpb.KeyValuePair {
	if db == nil {
		return nil
	}
	return db.Properties
}

// stringParamFold is stringParam with a case-insensitive key, for function
// parameters, whose keys the runtime lowercases before matching.
func stringParamFold(kvs []*commonpb.KeyValuePair, key string) (string, bool) {
	for _, kv := range kvs {
		if strings.EqualFold(kv.GetKey(), key) {
			return kv.GetValue(), true
		}
	}
	return "", false
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

// isServerField reports the fields the server adds to every collection rather
// than the user declaring them: RowID and Timestamp, and the namespace field
// added when namespaces are enabled, and the virtual primary key the Proxy
// adds to an external collection. Counting them would report every such
// collection as an Int64 (and auto_id) user, and their existence is already
// what the enable_namespace and external_collection predicates report.
func isServerField(f *model.Field) bool {
	return common.IsSystemField(f.FieldID) || f.Name == common.NamespaceFieldName || f.Name == common.VirtualPKFieldName
}
