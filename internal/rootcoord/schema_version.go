package rootcoord

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// schema.Version is the single monotonic schema version that BOTH QueryNode and
// segcore gate on: segcore's Collection::UpdateSchema rejects any payload whose
// version is not strictly greater, and QueryNode's prepareCollectionSchemaUpdate
// drops a same-version payload as a no-op. Two invariants follow, and both have to
// hold or a DDL is silently lost:
//
//	I1  broadcast carries FieldMaskCollectionSchema  =>  schema.Version = current + 1
//	I2  the schema snapshot's content changed        =>  broadcast carries
//	                                                     FieldMaskCollectionSchema
//
// I1 is kept structurally: nextSchemaSnapshot is the only place in rootcoord that
// computes the +1, so no DDL callback can forget it (TestSchemaVersionBumpHasSingleEntry
// guards that no other site does).
//
// I2 is kept by deriving the decision from CONTENT rather than from a list of
// property keys, so a property added later is covered without anyone remembering to
// register it. See projectSchemaProperties for the one place content is narrowed.

// nextSchemaSnapshot builds the schema snapshot a DDL is about to broadcast, with
// schema.Version already advanced. Every schema-carrying broadcast must come from
// here; callers then mutate the returned snapshot's own fields, never its Version.
func nextSchemaSnapshot(coll *model.Collection) *schemapb.CollectionSchema {
	schema := coll.ToCollectionSchemaPB()
	schema.Version = coll.SchemaVersion + 1
	return schema
}

// coordOnlyPropertyKeys are the collection properties that never reach QueryNode's
// effective schema. QueryCoord reads them straight off the collection meta and
// already has a dedicated channel for them (getAlterLoadConfigOfAlterCollection), so
// routing them through the schema snapshot buys nothing and costs a lot: every
// schema.Version bump fences all in-flight segment loads on the shard
// (shardDelegator.addDistributionIfSchemaVersionOK) into a retry, and these two keys
// are driven by elastic resize rather than by a human running DDL.
//
// This is deliberately a DENY-list, not an allow-list. An unregistered new property
// lands in the snapshot and refreshes normally, so the cost of forgetting to classify
// one is an extra broadcast — never a silently dropped setting, which is the failure
// mode that shipped mmap/warmup changes that only took effect after release+load.
var coordOnlyPropertyKeys = typeutil.NewSet(
	common.CollectionReplicaNumber,
	common.CollectionResourceGroups,
)

// projectSchemaProperties drops the QueryCoord-only properties from a property set
// destined for a schema snapshot.
func projectSchemaProperties(props []*commonpb.KeyValuePair) common.KeyValuePairs {
	return lo.Filter(props, func(kv *commonpb.KeyValuePair, _ int) bool {
		return !coordOnlyPropertyKeys.Contain(kv.GetKey())
	})
}

// schemaPropertiesChanged reports whether a property alter changes what QueryNode
// would see in the schema snapshot. Order-insensitive: newPropsKeyValuePairs is built
// from a map and carries no stable ordering.
func schemaPropertiesChanged(oldProps, newProps []*commonpb.KeyValuePair) bool {
	return !projectSchemaProperties(newProps).Equal(projectSchemaProperties(oldProps))
}
