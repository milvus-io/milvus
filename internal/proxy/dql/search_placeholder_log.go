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
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Limit metadata scanning independently of payload size (including empty values).
const maxPlaceholderLogFields = 65536

const placeholderLogTruncated = " <truncated>"

// One writer owns the budget for the entire request, including metadata, quoted
// text, delimiters and the truncation marker. Tokens are written incrementally;
// formatting stops as soon as the budget is exhausted.
type placeholderLogWriter struct {
	strings.Builder
	limit     int
	reserved  int
	truncated bool
}

func newPlaceholderLogWriter(limit int) *placeholderLogWriter {
	return &placeholderLogWriter{limit: max(0, limit), reserved: len(placeholderLogTruncated)}
}

func (w *placeholderLogWriter) write(token string) bool {
	if w.truncated {
		return false
	}
	if len(token) > w.limit-w.Len()-w.reserved {
		w.truncated = true
		return false
	}
	w.WriteString(token)
	return true
}

func (w *placeholderLogWriter) open(prefix, suffix string) bool {
	w.reserved += len(suffix)
	if !w.write(prefix) {
		w.reserved -= len(suffix)
		return false
	}
	return true
}

func (w *placeholderLogWriter) close(suffix string) {
	w.reserved -= len(suffix)
	w.WriteString(suffix) // Space was reserved by open, even if the body was truncated.
}

func (w *placeholderLogWriter) finish() string {
	if w.truncated {
		w.WriteString(placeholderLogTruncated[:min(w.limit, len(placeholderLogTruncated))])
	}
	return w.String()
}

func logSearchPlaceholders(ctx context.Context, log *mlog.Logger, request *milvuspb.SearchRequest, queryInfos []*planpb.QueryInfo) {
	limit := paramtable.Get().ProxyCfg.SearchPlaceholderLogRateLimit.GetAsFloat()
	// Check before RatedInfo: its burst token would otherwise allow a zero-rate log.
	if limit > 0 {
		log.RatedInfo(ctx, rate.Limit(limit), "proxy search request placeholder",
			mlog.Stringer("placeholder", searchPlaceholderStringer{
				request: request, queryInfos: queryInfos,
				maxBytes: paramtable.Get().ProxyCfg.SearchPlaceholderLogMaxBytes.GetAsInt(),
			}))
	}
}

// Rendering is deferred until the shared request-level limiter admits the entry.
type searchPlaceholderStringer struct {
	request    *milvuspb.SearchRequest
	queryInfos []*planpb.QueryInfo
	maxBytes   int
}

func (s searchPlaceholderStringer) String() string {
	w := newPlaceholderLogWriter(s.maxBytes)
	subReqs := s.request.GetSubReqs()
	count := max(1, len(subReqs))
	w.write(fmt.Sprintf("sub_requests=%d ", len(subReqs)))
	for i := 0; i < count && !w.truncated; i++ {
		placeholder, nq := s.request.GetPlaceholderGroup(), s.request.GetNq()
		if len(subReqs) > 0 {
			placeholder, nq = subReqs[i].GetPlaceholderGroup(), subReqs[i].GetNq()
		}
		var fieldID int64
		if i < len(s.queryInfos) {
			fieldID = s.queryInfos[i].GetQueryFieldId()
		}
		if !w.open(fmt.Sprintf("[index=%d anns_field_id=%d nq=%d bytes=%d ",
			i, fieldID, nq, len(placeholder)), "] ") {
			break
		}
		writePlaceholderGroup(w, placeholder)
		w.close("] ")
	}
	return w.finish()
}

// Inspect protobuf field headers without unmarshalling/copying the entire payload.
// Only the first value is retained as a view. Its contents are decoded one element
// at a time, using the remaining request-wide output budget.
func writePlaceholderGroup(w *placeholderLogWriter, data []byte) {
	groups := 0
	for fields := 0; len(data) > 0 && !w.truncated; fields++ {
		if fields == maxPlaceholderLogFields {
			w.write("<placeholder metadata exceeds scan limit>")
			break
		}
		num, typ, value, rest, ok := consumePlaceholderLogField(data)
		if !ok {
			w.write("<invalid placeholder group>")
			break
		}
		data = rest
		if num != 1 {
			continue
		}
		if typ != protowire.BytesType {
			w.write("<invalid placeholder group>")
			break
		}
		if groups > 0 {
			w.write(" | ")
		}
		if !w.truncated {
			writePlaceholderValue(w, value)
		}
		groups++
	}
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

func writePlaceholderValue(w *placeholderLogWriter, data []byte) {
	var phType commonpb.PlaceholderType
	var first []byte
	nq, size := 0, len(data)
	for fields := 0; len(data) > 0; fields++ {
		if fields == maxPlaceholderLogFields {
			w.write("<placeholder metadata exceeds scan limit>")
			return
		}
		num, typ, value, rest, ok := consumePlaceholderLogField(data)
		if !ok {
			w.write("<invalid placeholder>")
			return
		}
		data = rest
		switch num {
		case 2: // PlaceholderValue.type
			if typ != protowire.VarintType {
				w.write("<invalid placeholder type>")
				return
			}
			v, _ := protowire.ConsumeVarint(value)
			phType = commonpb.PlaceholderType(v)
		case 3: // PlaceholderValue.values
			if typ != protowire.BytesType {
				w.write("<invalid placeholder values>")
				return
			}
			nq++
			if nq == 1 {
				first = value
			}
		}
	}

	if !w.open(fmt.Sprintf("[type=%s nq=%d bytes=%d omitted_queries=%d", phType, nq, size, max(0, nq-1)), "]") {
		return
	}
	defer w.close("]")
	if nq > 0 {
		length := len(first)
		switch phType {
		case commonpb.PlaceholderType_FloatVector:
			w.write(fmt.Sprintf(" dim=%d", length/4))
		case commonpb.PlaceholderType_Float16Vector, commonpb.PlaceholderType_BFloat16Vector:
			w.write(fmt.Sprintf(" dim=%d", length/2))
		case commonpb.PlaceholderType_BinaryVector:
			w.write(fmt.Sprintf(" dim=%d", length*8))
		case commonpb.PlaceholderType_Int8Vector:
			w.write(fmt.Sprintf(" dim=%d", length))
		case commonpb.PlaceholderType_SparseFloatVector:
			w.write(fmt.Sprintf(" first_nnz=%d", length/8))
		default:
			w.write(fmt.Sprintf(" first_value_bytes=%d", length))
		}
	}
	if !w.open(" preview=[", "]") {
		return
	}
	defer w.close("]")
	if nq > 0 {
		writePlaceholderFirstQuery(w, phType, first)
	}
}

func writePlaceholderFirstQuery(w *placeholderLogWriter, phType commonpb.PlaceholderType, value []byte) {
	if phType == commonpb.PlaceholderType_VarChar {
		if !w.open(`"`, `"`) {
			return
		}
		defer w.close(`"`)
		for len(value) > 0 {
			_, width := utf8.DecodeRune(value)
			quoted := strconv.Quote(string(value[:width]))
			if !w.write(quoted[1 : len(quoted)-1]) {
				return
			}
			value = value[width:]
		}
		return
	}
	if phType == commonpb.PlaceholderType_Int64 {
		if v, err := typeutil.BytesToInt64(value); err == nil {
			w.write(strconv.FormatInt(v, 10))
		} else {
			w.write("<invalid int64>")
		}
		return
	}

	width, prefix := 1, "["
	switch phType {
	case commonpb.PlaceholderType_FloatVector:
		width = 4
	case commonpb.PlaceholderType_Float16Vector, commonpb.PlaceholderType_BFloat16Vector:
		width = 2
	case commonpb.PlaceholderType_SparseFloatVector:
		width, prefix = 8, "map["
	case commonpb.PlaceholderType_Int8Vector:
	default:
		// Binary vectors and opaque/embedding-list payloads use a byte preview.
		prefix = "bytes=["
	}
	if !w.open(prefix, "]") {
		return
	}
	defer w.close("]")
	for i := 0; i+width <= len(value); i += width {
		var token string
		switch phType {
		case commonpb.PlaceholderType_FloatVector:
			token = strconv.FormatFloat(float64(typeutil.BytesToFloat32(value[i:])), 'g', -1, 32)
		case commonpb.PlaceholderType_Float16Vector:
			token = strconv.FormatFloat(float64(typeutil.Float16BytesToFloat32(value[i:])), 'g', -1, 32)
		case commonpb.PlaceholderType_BFloat16Vector:
			token = strconv.FormatFloat(float64(typeutil.BFloat16BytesToFloat32(value[i:])), 'g', -1, 32)
		case commonpb.PlaceholderType_SparseFloatVector:
			token = strconv.FormatUint(uint64(typeutil.SparseFloatRowIndexAt(value, i/8)), 10) + ":" +
				strconv.FormatFloat(float64(typeutil.SparseFloatRowValueAt(value, i/8)), 'g', -1, 32)
		case commonpb.PlaceholderType_Int8Vector:
			token = strconv.FormatInt(int64(int8(value[i])), 10)
		default:
			token = strconv.FormatUint(uint64(value[i]), 10)
		}
		if i > 0 {
			token = " " + token
		}
		if !w.write(token) {
			return
		}
	}
}
