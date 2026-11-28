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

package rootcoord

import (
	"bytes"
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/model"
	streamingbroadcaster "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (c *Core) prepareAlterCollectionAnalyzerFileResources(ctx context.Context, coll *model.Collection, schema *schemapb.CollectionSchema) ([]int64, error) {
	fileResourceIds, err := c.validateChangedAnalyzers(ctx, coll, schema)
	if err != nil {
		return nil, err
	}
	schema.FileResourceIds = fileResourceIds

	addedFileResourceIds, _ := diffFileResourceIDs(coll.FileResourceIds, schema.FileResourceIds)
	if err := reserveAlterCollectionFileResourceRefs(c.meta, coll.CollectionID, addedFileResourceIds); err != nil {
		return nil, err
	}
	return addedFileResourceIds, nil
}

func rollbackAlterCollectionAnalyzerFileResourceReservation(ctx context.Context, meta IMetaTable, collectionID int64, ids []int64, broadcastErr error) {
	if streamingbroadcaster.IsBroadcastTaskNotCreated(broadcastErr) {
		rollbackAlterCollectionFileResourceRefs(ctx, meta, collectionID, ids)
	}
}

// Compare decoded JSON without converting integer parameters through float64.
func sameAnalyzerParams(a, b string) bool {
	if a == b {
		return true
	}
	if !json.Valid([]byte(a)) || !json.Valid([]byte(b)) {
		return false
	}
	var left, right any
	decode := func(raw string, value *any) error {
		d := json.NewDecoder(bytes.NewBufferString(raw))
		d.UseNumber()
		return d.Decode(value)
	}
	if decode(a, &left) != nil || decode(b, &right) != nil {
		return false
	}
	x, _ := json.Marshal(left)
	y, _ := json.Marshal(right)
	return bytes.Equal(x, y)
}

func resolveSchemaAnalyzerPaths(schema, previous *schemapb.CollectionSchema) error {
	oldFields := make(map[string]*schemapb.FieldSchema)
	for _, field := range previous.GetFields() {
		oldFields[field.GetName()] = field
	}
	cfg := paramtable.Get()
	resolve := func(value, before string) (string, error) {
		if sameAnalyzerParams(value, before) {
			return value, nil
		}
		return analyzer.ResolveLocalPaths(value, cfg.FunctionCfg.LocalResourcePath.GetValue(), cfg.TLSAllowedPath.GetValue())
	}
	for _, field := range schema.GetFields() {
		if !typeutil.CreateFieldSchemaHelper(field).EnableAnalyzer() && !isMinHashAnalyzerInput(schema, field) {
			continue
		}
		oldParams := make(map[string]string)
		old := oldFields[field.GetName()]
		if old != nil && (typeutil.CreateFieldSchemaHelper(old).EnableAnalyzer() || isMinHashAnalyzerInput(previous, old)) {
			for _, kv := range old.GetTypeParams() {
				oldParams[kv.Key] = kv.Value
			}
		}
		for _, kv := range field.GetTypeParams() {
			if kv.Key != "analyzer_params" && (kv.Key != "multi_analyzer_params" || !typeutil.CreateFieldSchemaHelper(field).EnableAnalyzer()) {
				continue
			}
			if sameAnalyzerParams(kv.Value, oldParams[kv.Key]) {
				continue
			}
			if kv.Key == "analyzer_params" {
				value, err := analyzer.ResolveLocalPaths(kv.Value, cfg.FunctionCfg.LocalResourcePath.GetValue(), cfg.TLSAllowedPath.GetValue())
				if err != nil {
					return err
				}
				kv.Value = value
				continue
			}
			// A change to one named analyzer must not retarget unchanged legacy paths.
			var multi, oldMulti map[string]json.RawMessage
			var analyzers, oldAnalyzers map[string]json.RawMessage
			if json.Unmarshal([]byte(kv.Value), &multi) != nil || json.Unmarshal(multi["analyzers"], &analyzers) != nil {
				return merr.WrapErrParameterInvalidMsg("invalid multi analyzer parameters")
			}
			_ = json.Unmarshal([]byte(oldParams[kv.Key]), &oldMulti)
			_ = json.Unmarshal(oldMulti["analyzers"], &oldAnalyzers)
			for name, raw := range analyzers {
				value, err := resolve(string(raw), string(oldAnalyzers[name]))
				if err != nil {
					return err
				}
				analyzers[name] = json.RawMessage(value)
			}
			multi["analyzers"], _ = json.Marshal(analyzers)
			data, _ := json.Marshal(multi)
			kv.Value = string(data)
		}
	}
	return nil
}

func (c *Core) validateChangedAnalyzers(ctx context.Context, coll *model.Collection, schema *schemapb.CollectionSchema) ([]int64, error) {
	previous := coll.ToCollectionSchemaPB()
	if err := resolveSchemaAnalyzerPaths(schema, previous); err != nil {
		return nil, err
	}
	oldInfos, err := collectAnalyzerInfos(previous)
	if err != nil {
		return nil, err
	}
	infos, err := collectAnalyzerInfos(schema)
	if err != nil {
		return nil, err
	}
	old := make(map[[2]string]string, len(oldInfos))
	for _, info := range oldInfos {
		old[[2]string{info.Field, info.Name}] = info.Params
	}
	changed := make([]*querypb.AnalyzerInfo, 0, len(infos))
	var retained []string
	for _, info := range infos {
		before, exists := old[[2]string{info.Field, info.Name}]
		if exists && sameAnalyzerParams(before, info.Params) {
			retained = append(retained, info.Params)
		} else {
			changed = append(changed, info)
		}
	}
	added, err := c.validateAnalyzerInfos(ctx, changed)
	if err != nil {
		return nil, err
	}
	ids := typeutil.NewSet[int64](added...)
	names := fileresource.AnalyzerResourceNames(retained)
	if len(names) != 0 {
		var resources []*internalpb.FileResourceInfo
		// Old schemas without ID bindings retain their existing name lookup behavior.
		if len(coll.FileResourceIds) == 0 {
			for name := range names {
				if resource := c.meta.FindFileResource(ctx, name); resource != nil {
					resources = append(resources, resource)
				}
			}
		} else {
			resources, err = c.meta.GetFileResources(ctx, coll.FileResourceIds...)
		}
		if err != nil {
			return nil, err
		}
		// Syntactic candidates can include ignored options; only retain existing bindings.
		for _, resource := range resources {
			if _, used := names[resource.GetName()]; used {
				ids.Insert(resource.GetId())
			}
		}
	}
	return ids.Collect(), nil
}

// Admission precedes ID assignment, so supplied IDs cannot hide a named input.
func isMinHashAnalyzerInput(schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) bool {
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() == schemapb.FunctionType_MinHash &&
			(slices.Contains(fn.GetInputFieldNames(), field.GetName()) ||
				(field.GetFieldID() != 0 && slices.Contains(fn.GetInputFieldIds(), field.GetFieldID()))) {
			return true
		}
	}
	return false
}
