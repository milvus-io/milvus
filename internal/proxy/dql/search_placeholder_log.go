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

package dql

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	maxPlaceholderLogBytes      = 16 * 1024
	maxPlaceholderLogGroups     = 4
	maxPlaceholderLogValues     = 1
	maxPlaceholderLogValueBytes = 128
	maxPlaceholderLogFields     = 65536
)

func logSearchPlaceholders(ctx context.Context, log *mlog.Logger, request *milvuspb.SearchRequest, queryInfos []*planpb.QueryInfo) {
	limit := paramtable.Get().ProxyCfg.SearchPlaceholderLogRateLimit.GetAsFloat()
	// Check before RatedInfo: its burst token would otherwise allow a zero-rate log.
	if limit > 0 {
		log.RatedInfo(ctx, rate.Limit(limit), "proxy search request placeholder",
			mlog.Stringer("placeholder", searchPlaceholderStringer{request: request, queryInfos: queryInfos}))
	}
}

// Rendering is deferred until the shared request-level limiter admits the entry.
type searchPlaceholderStringer struct {
	request    *milvuspb.SearchRequest
	queryInfos []*planpb.QueryInfo
}

func (s searchPlaceholderStringer) String() string {
	var sb strings.Builder
	subReqs := s.request.GetSubReqs()
	count := max(1, len(subReqs))
	fmt.Fprintf(&sb, "sub_requests=%d ", len(subReqs))
	for i := 0; i < count; i++ {
		placeholder, nq := s.request.GetPlaceholderGroup(), s.request.GetNq()
		if len(subReqs) > 0 {
			placeholder, nq = subReqs[i].GetPlaceholderGroup(), subReqs[i].GetNq()
		}
		var fieldID int64
		if i < len(s.queryInfos) {
			fieldID = s.queryInfos[i].GetQueryFieldId()
		}
		entry := fmt.Sprintf("[index=%d anns_field_id=%d nq=%d bytes=%d %s] ",
			i, fieldID, nq, len(placeholder), placeholderGroupToLogString(placeholder))
		// Leave room for the omitted-request count; never cut a quoted text value.
		if sb.Len()+len(entry) > maxPlaceholderLogBytes-128 {
			fmt.Fprintf(&sb, "<truncated: omitted_sub_requests=%d>", count-i)
			break
		}
		sb.WriteString(entry)
	}
	return sb.String()
}

// Inspect protobuf field headers without unmarshalling/copying the entire payload.
// Only the first value is retained as a view; only its first 128 bytes are
// decoded/formatted. A large nq or dimension therefore cannot cause large allocations.
func placeholderGroupToLogString(data []byte) string {
	var sb strings.Builder
	groups := 0
	for fields := 0; len(data) > 0; fields++ {
		if groups == maxPlaceholderLogGroups || fields == maxPlaceholderLogFields {
			sb.WriteString("<truncated placeholders>")
			break
		}
		num, typ, value, rest, ok := consumePlaceholderLogField(data)
		if !ok {
			sb.WriteString("<invalid placeholder group>")
			break
		}
		data = rest
		if num != 1 {
			continue
		}
		if typ != protowire.BytesType {
			sb.WriteString("<invalid placeholder group>")
			break
		}
		if groups > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(placeholderValueToLogString(value))
		groups++
	}
	return sb.String()
}

// The returned value aliases the input. Groups are not part of PlaceholderGroup's
// schema; do not recursively inspect them just to produce a diagnostic preview.
func consumePlaceholderLogField(data []byte) (protowire.Number, protowire.Type, []byte, []byte, bool) {
	num, typ, n := protowire.ConsumeTag(data)
	if n < 0 || typ == protowire.StartGroupType || typ == protowire.EndGroupType {
		return 0, 0, nil, nil, false
	}
	data = data[n:]
	m := protowire.ConsumeFieldValue(num, typ, data)
	if m < 0 {
		return 0, 0, nil, nil, false
	}
	value := data[:m]
	if typ == protowire.BytesType {
		value, _ = protowire.ConsumeBytes(data)
	}
	return num, typ, value, data[m:], true
}

func placeholderValueToLogString(data []byte) string {
	var phType commonpb.PlaceholderType
	var values [][]byte
	nq, size := 0, len(data)
	for fields := 0; len(data) > 0; fields++ {
		if fields == maxPlaceholderLogFields {
			return "<placeholder metadata exceeds preview limit>"
		}
		num, typ, value, rest, ok := consumePlaceholderLogField(data)
		if !ok {
			return "<invalid placeholder>"
		}
		data = rest
		switch num {
		case 2: // PlaceholderValue.type
			if typ != protowire.VarintType {
				return "<invalid placeholder type>"
			}
			v, _ := protowire.ConsumeVarint(value)
			phType = commonpb.PlaceholderType(v)
		case 3: // PlaceholderValue.values
			if typ != protowire.BytesType {
				return "<invalid placeholder values>"
			}
			nq++
			if len(values) < maxPlaceholderLogValues {
				values = append(values, value)
			}
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "[type=%s nq=%d bytes=%d", phType, nq, size)
	if len(values) > 0 {
		length := len(values[0])
		switch phType {
		case commonpb.PlaceholderType_FloatVector:
			fmt.Fprintf(&sb, " dim=%d", length/4)
		case commonpb.PlaceholderType_Float16Vector, commonpb.PlaceholderType_BFloat16Vector:
			fmt.Fprintf(&sb, " dim=%d", length/2)
		case commonpb.PlaceholderType_BinaryVector:
			fmt.Fprintf(&sb, " dim=%d", length*8)
		case commonpb.PlaceholderType_Int8Vector:
			fmt.Fprintf(&sb, " dim=%d", length)
		case commonpb.PlaceholderType_SparseFloatVector:
			fmt.Fprintf(&sb, " first_nnz=%d", length/8)
		default:
			fmt.Fprintf(&sb, " first_value_bytes=%d", length)
		}
	}
	sb.WriteString(" preview=[")
	truncated := nq > len(values)
	for i, value := range values {
		if i > 0 {
			sb.WriteByte(' ')
		}
		truncated = truncated || len(value) > maxPlaceholderLogValueBytes
		sample := value[:min(len(value), maxPlaceholderLogValueBytes)]
		switch phType {
		case commonpb.PlaceholderType_VarChar:
			fmt.Fprintf(&sb, "%q", sample)
		case commonpb.PlaceholderType_Int64:
			if v, err := typeutil.BytesToInt64(sample); err == nil {
				fmt.Fprint(&sb, v)
			} else {
				sb.WriteString("<invalid int64>")
			}
		case commonpb.PlaceholderType_FloatVector:
			fmt.Fprint(&sb, typeutil.BytesToFloat32Vector(sample))
		case commonpb.PlaceholderType_Float16Vector:
			fmt.Fprint(&sb, typeutil.Float16BytesToFloat32Vector(sample))
		case commonpb.PlaceholderType_BFloat16Vector:
			fmt.Fprint(&sb, typeutil.BFloat16BytesToFloat32Vector(sample))
		case commonpb.PlaceholderType_Int8Vector:
			vec := make([]int8, len(sample))
			for j, v := range sample {
				vec[j] = int8(v)
			}
			fmt.Fprint(&sb, vec)
		case commonpb.PlaceholderType_SparseFloatVector:
			fmt.Fprint(&sb, typeutil.SparseFloatBytesToMap(sample))
		default:
			// Binary vectors and opaque/embedding-list payloads use a byte preview.
			fmt.Fprintf(&sb, "bytes=%v", sample)
		}
	}
	fmt.Fprintf(&sb, "] truncated=%t]", truncated)
	return sb.String()
}
