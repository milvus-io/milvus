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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func isNoTrainIndex(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsNoTrainIndex(indexType)
}

func isMvSupported(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsMvSupported(indexType)
}

func isDiskANNIndex(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType)
}

func getIndexParam(indexParams []*commonpb.KeyValuePair, key string) (string, error) {
	for _, p := range indexParams {
		if p.Key == key {
			return p.Value, nil
		}
	}
	return "", merr.WrapErrParameterInvalidMsg("%s not found", key)
}

func isTypedJSONArrayIndex(indexType, castType string) bool {
	if indexType != indexparamcheck.IndexHybrid && indexType != indexparamcheck.IndexSTLSORT {
		return false
	}
	return indexparamcheck.IsJSONArrayCastType(castType)
}

func checkJSONArrayIndexEnabled(indexType, castType string) error {
	if isTypedJSONArrayIndex(indexType, castType) &&
		!Params.DataCoordCfg.EnableArraySortIndex.GetAsBool() {
		return merr.WrapErrServiceNotReadyMsg("JSON ARRAY HYBRID and STL_SORT are disabled by the cluster software upgrade gate")
	}
	return nil
}

// Explicit SORT on ordinary ARRAY is a new logical index type for old readers.
// Nested ARRAY SORT retains its existing admission rules.
func checkOrdinaryArraySortIndex(field *schemapb.FieldSchema, indexType string, versions IndexEngineVersionManager) error {
	if indexType != indexparamcheck.IndexSTLSORT || field.GetDataType() != schemapb.DataType_Array || typeutil.IsStructSubField(field.GetName()) {
		return nil
	}
	if !Params.DataCoordCfg.EnableArraySortIndex.GetAsBool() {
		return merr.WrapErrServiceNotReadyMsg("ARRAY STL_SORT is disabled by the cluster software upgrade gate")
	}
	if versions.ResolveScalarIndexVersion() < 3 {
		return merr.WrapErrParameterInvalidMsg("ARRAY STL_SORT requires the existing packed scalar index format (version >= 3)")
	}
	return nil
}

func requiredJSONPathIndexVersion(indexType, castType string) int32 {
	if isTypedJSONArrayIndex(indexType, castType) {
		return 3 // Existing packed scalar index format; no new engine version.
	}
	return common.MinScalarIndexVersionForJsonPathMultiType
}

// checkFMIndexEngineVersion rejects FMINDEX creation when the cluster's resolved
// scalar index engine version is below MinScalarIndexVersionForFMINDEX. FMINDEX
// (scalar engine v5) is a new capability older nodes cannot handle:
// `ResolveScalarIndexVersion` aggregates QueryNode sessions, so passing here
// means no old QueryNode will be asked to LOAD a freshly built FMINDEX segment.
//
// This is shared by BOTH Server.CreateIndex and snapshotManager.RestoreIndexes:
// restore broadcasts the same CreateIndex DDL directly, so without this it could
// bypass the gate and create an FMINDEX index in a mixed-version cluster.
// FMINDEX is opt-in (never an AutoIndex-resolved type), so reject, don't downgrade.
//
// Caveat (unchanged): this gates the loaders, not the build workers
// (DataNode/IndexNode), which must be upgraded no later than the QueryNodes; the
// bundled upgrade script orders IndexNode/DataNode first. Gating build workers on
// scalar capability is a follow-up.
func checkFMIndexEngineVersion(indexParams []*commonpb.KeyValuePair, resolvedScalarVersion int32) error {
	if common.GetIndexType(indexParams) != indexparamcheck.IndexFMINDEX {
		return nil
	}
	if resolvedScalarVersion < common.MinScalarIndexVersionForFMINDEX {
		return merr.WrapErrServiceNotReadyMsg(
			"FMINDEX requires scalar index engine version >= %d, current resolved version: %d (a rolling upgrade may still be in progress)",
			common.MinScalarIndexVersionForFMINDEX, resolvedScalarVersion)
	}
	return nil
}

func DeleteParams(from []*commonpb.KeyValuePair, deletes []string) []*commonpb.KeyValuePair {
	params := make(map[string]string)
	for _, param := range from {
		params[param.GetKey()] = param.GetValue()
	}

	// delete the params
	for _, key := range deletes {
		delete(params, key)
	}

	return lo.MapToSlice(params, func(k string, v string) *commonpb.KeyValuePair {
		return &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		}
	})
}
