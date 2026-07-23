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

package validator

import (
	"slices"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// alterableFunctionParams lists, per function type, the param keys that
// alter_function may change. Only pure connection/auth/perf knobs are allowed —
// params that never change the produced vector for a given input. Any param that
// can change the output vector's semantics or shape is immutable, because altering
// it on an already-backfilled output field would silently mix incompatible vectors
// in the same field. A function type absent from this map (BM25, MinHash) has no
// alterable params.
//
// Immutable (semantic) params, by first principle "same input must keep producing a
// compatible vector":
//   - dim / model_name / provider / normalization / prompts: change the model or its
//     output shape.
//   - endpoint: for TEI the endpoint IS the model's identity, so repointing it can
//     silently swap to a different model.
//   - truncate / truncation_direction: for an over-length input they change which
//     tokens are embedded (truncate on/off, head vs tail), so the produced vector
//     differs — altering them mixes old and new vector semantics in one field.
//
// url stays alterable because model_name providers keep their identity in model_name
// and use url only as a relocatable API host.
var alterableFunctionParams = map[schemapb.FunctionType]typeutil.Set[string]{
	schemapb.FunctionType_TextEmbedding: typeutil.NewSet(
		models.URLParamKey,
		models.CredentialParamKey,
		models.TimeoutMsParamKey,
		models.MaxClientBatchSizeParamKey,
		models.RegionParamKey,
		models.LocationParamKey,
		models.ProjectIDParamKey,
		models.UserParamKey,
	),
}

// CheckFunctionAlterAllowed rejects an alter_function request that changes
// anything other than whitelisted params. The function identity (type, name,
// input/output fields) is immutable; only connection/runtime params may change.
func CheckFunctionAlterAllowed(oldFn, newFn *schemapb.FunctionSchema) error {
	if oldFn.GetType() != newFn.GetType() {
		return merr.WrapErrParameterInvalidMsg("function type cannot be altered: function %s", oldFn.GetName())
	}
	if oldFn.GetName() != newFn.GetName() {
		return merr.WrapErrParameterInvalidMsg("function name cannot be altered: %s -> %s", oldFn.GetName(), newFn.GetName())
	}
	if !slices.Equal(oldFn.GetInputFieldNames(), newFn.GetInputFieldNames()) {
		return merr.WrapErrParameterInvalidMsg("function input fields cannot be altered: function %s", oldFn.GetName())
	}
	if !slices.Equal(oldFn.GetOutputFieldNames(), newFn.GetOutputFieldNames()) {
		return merr.WrapErrParameterInvalidMsg("function output fields cannot be altered: function %s", oldFn.GetName())
	}

	whitelist := alterableFunctionParams[oldFn.GetType()]
	if len(whitelist) == 0 {
		return merr.WrapErrParameterInvalidMsg("function type %s has no alterable params", oldFn.GetType().String())
	}
	oldParams, err := normalizeParams(oldFn.GetParams())
	if err != nil {
		return err
	}
	newParams, err := normalizeParams(newFn.GetParams())
	if err != nil {
		return err
	}
	changed := typeutil.NewSet[string]()
	for k, ov := range oldParams {
		if nv, ok := newParams[k]; !ok || nv != ov {
			changed.Insert(k)
		}
	}
	for k := range newParams {
		if _, ok := oldParams[k]; !ok {
			changed.Insert(k)
		}
	}
	for k := range changed {
		if !whitelist.Contain(k) {
			return merr.WrapErrParameterInvalidMsg(
				"function param %q cannot be altered for function %s; only connection/runtime params may be changed", k, oldFn.GetName())
		}
	}
	return nil
}

// normalizeParams lowercases param keys (matching how providers read them, e.g.
// getProvider) and rejects duplicate keys, so a crafted duplicate key cannot slip
// a non-whitelisted change past the diff while the runtime binds to a different
// occurrence.
func normalizeParams(params []*commonpb.KeyValuePair) (map[string]string, error) {
	m := make(map[string]string, len(params))
	for _, p := range params {
		k := strings.ToLower(p.GetKey())
		if _, dup := m[k]; dup {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate function param key %q", p.GetKey())
		}
		m[k] = p.GetValue()
	}
	return m, nil
}
