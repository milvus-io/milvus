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

package datacoord

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// importSchemaProjection keeps only schema facts that can change Import V3's
// physical output or function execution. Presentation-only names/descriptions,
// index membership and schema version are omitted.
type importSchemaProjection struct {
	Fields          []*schemapb.FieldSchema
	StructFields    []*schemapb.StructArrayFieldSchema
	Functions       []*schemapb.FunctionSchema
	Properties      []*commonpb.KeyValuePair
	EnableDynamic   bool
	EnableNamespace bool
	FileResourceIDs []int64
	DbName          string
}

// The projection is compared through a deterministic, ordinary Go value rather
// than adding another wire protocol.  Clone helpers below strip display-only
// fields before proto.Equal is used on the nested schema messages.
func compareImportSchemaProjection(frozen, current *schemapb.CollectionSchema) (bool, string) {
	if frozen == nil || current == nil {
		return false, "schema is missing"
	}
	frozenProjection := buildImportSchemaProjection(frozen)
	currentProjection := buildImportSchemaProjection(current)
	if frozenProjection.equal(currentProjection) {
		return true, ""
	}
	return false, fmt.Sprintf("frozen schema version %d is not import-compatible with current version %d", frozen.GetVersion(), current.GetVersion())
}

func buildImportSchemaProjection(schema *schemapb.CollectionSchema) *importSchemaProjection {
	projection := &importSchemaProjection{
		EnableDynamic:   schema.GetEnableDynamicField(),
		EnableNamespace: schema.GetEnableNamespace(),
		FileResourceIDs: append([]int64(nil), schema.GetFileResourceIds()...),
		DbName:          schema.GetDbName(),
	}
	for _, field := range schema.GetFields() {
		if common.IsSystemField(field.GetFieldID()) {
			// System fields (RowID/Timestamp) are appended by rootcoord on
			// CreateCollection but are not part of the user-visible import
			// schema the proxy forwards; ignore them for projection equality.
			continue
		}
		cloned := proto.Clone(field).(*schemapb.FieldSchema)
		cloned.Name = ""
		cloned.Description = ""
		cloned.IndexParams = nil
		projection.Fields = append(projection.Fields, cloned)
	}
	for _, field := range schema.GetStructArrayFields() {
		cloned := proto.Clone(field).(*schemapb.StructArrayFieldSchema)
		cloned.Name = ""
		cloned.Description = ""
		for _, nested := range cloned.GetFields() {
			nested.Name = ""
			nested.Description = ""
			nested.IndexParams = nil
		}
		projection.StructFields = append(projection.StructFields, cloned)
	}
	for _, function := range schema.GetFunctions() {
		cloned := proto.Clone(function).(*schemapb.FunctionSchema)
		cloned.Name = ""
		cloned.Description = ""
		cloned.InputFieldNames = nil
		cloned.OutputFieldNames = nil
		projection.Functions = append(projection.Functions, cloned)
	}
	for _, property := range schema.GetProperties() {
		switch property.GetKey() {
		case common.CollectionTTLConfigKey,
			common.CollectionTTLFieldKey,
			common.TimezoneKey,
			common.CollectionAllowInsertNonBM25FunctionOutputs,
			common.NamespaceModeKey,
			common.NamespaceShardingEnabledKey:
			projection.Properties = append(projection.Properties, proto.Clone(property).(*commonpb.KeyValuePair))
		}
	}
	sort.Slice(projection.Fields, func(i, j int) bool { return projection.Fields[i].GetFieldID() < projection.Fields[j].GetFieldID() })
	sort.Slice(projection.StructFields, func(i, j int) bool {
		return projection.StructFields[i].GetFieldID() < projection.StructFields[j].GetFieldID()
	})
	sort.Slice(projection.Functions, func(i, j int) bool { return projection.Functions[i].GetId() < projection.Functions[j].GetId() })
	sort.Slice(projection.Properties, func(i, j int) bool { return projection.Properties[i].GetKey() < projection.Properties[j].GetKey() })
	sort.Slice(projection.FileResourceIDs, func(i, j int) bool { return projection.FileResourceIDs[i] < projection.FileResourceIDs[j] })
	return projection
}

func (p *importSchemaProjection) equal(other *importSchemaProjection) bool {
	if p == nil || other == nil || p.EnableDynamic != other.EnableDynamic || p.EnableNamespace != other.EnableNamespace || p.DbName != other.DbName {
		return false
	}
	if len(p.Fields) != len(other.Fields) || len(p.StructFields) != len(other.StructFields) || len(p.Functions) != len(other.Functions) ||
		len(p.Properties) != len(other.Properties) || len(p.FileResourceIDs) != len(other.FileResourceIDs) {
		return false
	}
	for i := range p.Fields {
		if !proto.Equal(p.Fields[i], other.Fields[i]) {
			return false
		}
	}
	for i := range p.StructFields {
		if !proto.Equal(p.StructFields[i], other.StructFields[i]) {
			return false
		}
	}
	for i := range p.Functions {
		if !proto.Equal(p.Functions[i], other.Functions[i]) {
			return false
		}
	}
	for i := range p.Properties {
		if !proto.Equal(p.Properties[i], other.Properties[i]) {
			return false
		}
	}
	for i := range p.FileResourceIDs {
		if p.FileResourceIDs[i] != other.FileResourceIDs[i] {
			return false
		}
	}
	return true
}

func validateImportV3Schema(meta *meta, collectionID int64, frozen *schemapb.CollectionSchema) (int32, error) {
	collection := meta.GetCollection(collectionID)
	if collection == nil || collection.Schema == nil {
		// The collection cache is populated asynchronously after a DataCoord
		// restart, so a miss means "not loaded yet", not "dropped" (the GC loop's
		// checkCollection detects real drops via broker.HasCollection). Return a
		// retryable error so the job waits for the cache to warm instead of being
		// failed permanently.
		return 0, merr.WrapErrServiceNotReadyMsg("import v3 collection %d schema is not loaded into cache yet", collectionID)
	}
	if equal, difference := compareImportSchemaProjection(frozen, collection.Schema); !equal {
		return 0, merr.WrapErrImportSysFailedMsg("import v3 schema projection mismatch: %s", difference)
	}
	return collection.Schema.GetVersion(), nil
}
