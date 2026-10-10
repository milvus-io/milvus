// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/iteratorutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func configureIteratorPKCursor(params []*commonpb.KeyValuePair, info *planpb.SearchIteratorV2Info, schema *schemapb.CollectionSchema) error {
	values := make(map[string]string, 3)
	for _, param := range params {
		switch param.GetKey() {
		case iteratorutil.CursorVersionKey, iteratorutil.LastPKTypeKey, iteratorutil.LastPKKey:
			if _, duplicate := values[param.GetKey()]; duplicate {
				return merr.WrapErrParameterInvalidMsg("duplicate iterator cursor parameter %q", param.GetKey())
			}
			values[param.GetKey()] = param.GetValue()
		}
	}
	version, hasVersion := values[iteratorutil.CursorVersionKey]
	kind, hasKind := values[iteratorutil.LastPKTypeKey]
	value, hasValue := values[iteratorutil.LastPKKey]
	if !hasVersion {
		if hasKind || hasValue {
			return merr.WrapErrParameterMissingMsg("primary-key continuation requires %s", iteratorutil.CursorVersionKey)
		}
		return nil
	}
	if version != iteratorutil.PKCursorVersionString {
		return merr.WrapErrParameterInvalidMsg("unsupported iterator cursor version %q", version)
	}
	if info == nil {
		return merr.WrapErrParameterInvalidMsg("primary-key continuation requires search iterator V2")
	}
	if hasKind != hasValue {
		return merr.WrapErrParameterMissingMsg("primary-key continuation requires both key type and key value")
	}
	info.CursorVersion = iteratorutil.PKCursorVersion
	if hasKind {
		pk, err := iteratorutil.ParsePrimaryKey(kind, value)
		if err != nil {
			return err
		}
		info.LastPk = pk
	}
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return iteratorutil.ValidateCursor(info, field.GetDataType())
		}
	}
	return merr.WrapErrServiceInternalMsg("resolved collection schema has no primary key for iterator cursor")
}

// Strict requests cannot use options that change scores or candidate shape.
// Reject the combination without switching an iterator to another ordering.
func declineIteratorPKCursor(info *planpb.SearchIteratorV2Info, reason string) error {
	if info.GetCursorVersion() != iteratorutil.PKCursorVersion {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg("search iterator primary-key continuation is not supported with %s", reason)
}

// BM25 rewrites the query with live IDF and average document length on every
// request. A read timestamp does not freeze those statistics across pages.
func validateIteratorPKScoring(info *planpb.QueryInfo, field *schemapb.FieldSchema, schema *schemapb.CollectionSchema) error {
	if info.GetSearchIteratorV2Info().GetCursorVersion() != iteratorutil.PKCursorVersion {
		return nil
	}
	if strings.EqualFold(info.GetMetricType(), "BM25") {
		return declineIteratorPKCursor(info.GetSearchIteratorV2Info(), "BM25 live scoring statistics")
	}
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 {
			continue
		}
		for _, outputID := range function.GetOutputFieldIds() {
			if outputID == field.GetFieldID() {
				return declineIteratorPKCursor(info.GetSearchIteratorV2Info(), "BM25 live scoring statistics")
			}
		}
	}
	return nil
}

// attachIteratorPKCursor advertises execution only after every participating
// worker acknowledged the new path. A first page may fall back to legacy mode
// in a mixed cluster; continuation must never silently change its ordering.
func attachIteratorPKCursor(result *milvuspb.SearchResults, info *planpb.SearchIteratorV2Info, workers []*internalpb.SearchResults) error {
	if info.GetCursorVersion() != iteratorutil.PKCursorVersion {
		return nil
	}
	if !iteratorutil.AllResultsUsePKCursor(workers) {
		if info.GetLastPk() != nil {
			return merr.Wrapf(merr.ErrServiceUnimplemented, "search iterator primary-key continuation is not supported by every participating worker")
		}
		return nil
	}
	data := result.GetResults()
	count := len(data.GetScores())
	if data.GetNumQueries() != 1 || len(data.GetTopks()) != 1 || data.GetTopks()[0] != int64(count) {
		return merr.WrapErrServiceInternalMsg("iterator result has an inconsistent single-query row count")
	}
	ids := data.GetIds()
	if count == 0 {
		if len(ids.GetIntId().GetData()) != 0 || len(ids.GetStrId().GetData()) != 0 {
			return merr.WrapErrServiceInternalMsg("empty iterator result contains primary keys")
		}
		if result.Status == nil {
			result.Status = merr.Success()
		}
		iteratorutil.MarkPKCursor(result.Status)
		return nil
	}
	for _, score := range data.GetScores() {
		if math.IsNaN(float64(score)) || math.IsInf(float64(score), 0) {
			return merr.WrapErrServiceInternalMsg("iterator result contains a non-finite score")
		}
	}
	var kind, value string
	switch id := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		if len(id.IntId.GetData()) != count {
			return merr.WrapErrServiceInternalMsg("iterator result primary-key count does not match score count")
		}
		kind = iteratorutil.Int64PKType
		value = strconv.FormatInt(id.IntId.Data[count-1], 10)
	case *schemapb.IDs_StrId:
		if len(id.StrId.GetData()) != count {
			return merr.WrapErrServiceInternalMsg("iterator result primary-key count does not match score count")
		}
		kind = iteratorutil.VarCharPKType
		value = id.StrId.Data[count-1]
	default:
		return merr.WrapErrServiceInternalMsg("iterator result has no supported primary-key data")
	}
	if result.Status == nil {
		result.Status = merr.Success()
	}
	iteratorutil.MarkPKCursor(result.Status)
	result.Status.ExtraInfo[iteratorutil.LastPKTypeKey] = kind
	result.Status.ExtraInfo[iteratorutil.LastPKKey] = value
	return nil
}
