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

package iteratorutil

import (
	"math"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	CursorVersionKey      = "search_iter_cursor_version"
	LastPKTypeKey         = "search_iter_last_pk_type"
	LastPKKey             = "search_iter_last_pk"
	PKCursorVersion       = uint32(2)
	PKCursorVersionString = "2"
	Int64PKType           = "int64"
	VarCharPKType         = "varchar"
)

// ParsePrimaryKey retains Int64 precision and literal VarChar values. The
// enclosing protobuf string handles escaping; these values are never DSL.
func ParsePrimaryKey(kind, value string) (*planpb.GenericValue, error) {
	switch kind {
	case Int64PKType:
		pk, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("invalid iterator Int64 primary key")
		}
		return &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: pk}}, nil
	case VarCharPKType:
		return &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: value}}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported iterator primary-key type %q", kind)
	}
}

// ValidateCursor checks the request against its resolved collection schema.
// Legacy plans intentionally retain their existing distance-only semantics.
func ValidateCursor(info *planpb.SearchIteratorV2Info, pkType schemapb.DataType) error {
	if info == nil || info.GetCursorVersion() == 0 {
		return nil
	}
	if info.GetCursorVersion() != PKCursorVersion {
		return merr.WrapErrParameterInvalidMsg("unsupported search iterator cursor version %d", info.GetCursorVersion())
	}
	hasBound, hasPK := info.LastBound != nil, info.LastPk != nil
	if hasBound != hasPK {
		return merr.WrapErrParameterInvalidMsg("iterator continuation requires both score and primary key")
	}
	if info.LastBound == nil {
		return nil
	}
	if math.IsNaN(float64(info.GetLastBound())) || math.IsInf(float64(info.GetLastBound()), 0) {
		return merr.WrapErrParameterInvalidMsg("iterator cursor score must be finite")
	}
	switch pkType {
	case schemapb.DataType_Int64:
		if _, ok := info.LastPk.GetVal().(*planpb.GenericValue_Int64Val); !ok {
			return merr.WrapErrParameterInvalidMsg("iterator primary-key type does not match the Int64 collection primary key")
		}
	case schemapb.DataType_VarChar:
		if _, ok := info.LastPk.GetVal().(*planpb.GenericValue_StringVal); !ok {
			return merr.WrapErrParameterInvalidMsg("iterator primary-key type does not match the VarChar collection primary key")
		}
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported collection primary-key type for iterator cursor")
	}
	return nil
}

func MarkPKCursor(status *commonpb.Status) {
	if status == nil {
		return
	}
	if status.ExtraInfo == nil {
		status.ExtraInfo = make(map[string]string)
	}
	status.ExtraInfo[CursorVersionKey] = PKCursorVersionString
}

// AllResultsUsePKCursor checks execution evidence, not a proxy's code version.
// An old worker without the marker prevents a mixed cluster from advertising
// an ordering guarantee that it did not execute.
func AllResultsUsePKCursor(results []*internalpb.SearchResults) bool {
	if len(results) == 0 {
		return false
	}
	for _, result := range results {
		status := result.GetStatus()
		if status == nil || status.GetCode() != 0 || status.GetErrorCode() != commonpb.ErrorCode_Success ||
			status.GetExtraInfo()[CursorVersionKey] != PKCursorVersionString {
			return false
		}
	}
	return true
}

func PropagatePKCursor(results []*internalpb.SearchResults, result *internalpb.SearchResults) {
	if !AllResultsUsePKCursor(results) {
		if result.Status != nil {
			delete(result.Status.ExtraInfo, CursorVersionKey)
			delete(result.Status.ExtraInfo, LastPKTypeKey)
			delete(result.Status.ExtraInfo, LastPKKey)
		}
		return
	}
	if result.Status == nil {
		result.Status = merr.Success()
	}
	MarkPKCursor(result.Status)
}
