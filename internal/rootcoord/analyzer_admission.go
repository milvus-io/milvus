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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

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
		if !typeutil.CreateFieldSchemaHelper(field).EnableAnalyzer() {
			continue
		}
		oldParams := make(map[string]string)
		old := oldFields[field.GetName()]
		if old != nil && (typeutil.CreateFieldSchemaHelper(old).EnableAnalyzer()) {
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
				value, err := resolve(kv.Value, oldParams[kv.Key])
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

func (c *Core) validateChangedAnalyzers(ctx context.Context, schema, previous *schemapb.CollectionSchema) error {
	if err := resolveSchemaAnalyzerPaths(schema, previous); err != nil {
		return err
	}
	old := make(map[[2]string]string)
	var infos []*querypb.AnalyzerInfo
	for _, field := range previous.GetFields() {
		if err := validateAnalyzer(previous, field, &infos); err != nil {
			return err
		}
	}
	for _, info := range infos {
		old[[2]string{info.Field, info.Name}] = info.Params
	}
	infos = nil
	for _, field := range schema.GetFields() {
		if err := validateAnalyzer(schema, field, &infos); err != nil {
			return err
		}
	}
	changed := make([]*querypb.AnalyzerInfo, 0, len(infos))
	for _, info := range infos {
		before, exists := old[[2]string{info.Field, info.Name}]
		if !exists || !sameAnalyzerParams(before, info.Params) {
			changed = append(changed, info)
		}
	}
	if len(changed) == 0 {
		return nil
	}
	response, err := c.mixCoord.ValidateAnalyzer(ctx, &querypb.ValidateAnalyzerRequest{AnalyzerInfos: changed})
	return merr.CheckRPCCall(response, err)
}
