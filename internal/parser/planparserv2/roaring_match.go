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

package planparserv2

// MRB1 format validation and decoded-memory accounting: the roaring kind's
// data plane. Visitor, fill, and tree walking live in membership_filter.go;
// only the roaring-specific admission gate stays here. See
// docs/design-docs/design_docs/20260714-roaring-exact-membership-expression.md.

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type validatedRoaringBitmapBlob struct {
	blob    []byte
	summary roaringfilter.ValidationSummary
}

// checkRoaringMatchField validates that the probe column is a plain top-level
// signed-integer field. Roaring indexes integers, so unlike bloom_match there is
// no VARCHAR or JSON path: a string would have to be hashed into the integer key
// space first, which would reintroduce the false positives roaring_match exists
// to avoid.
func checkRoaringMatchField(columnInfo *planpb.ColumnInfo, argText, functionName string) error {
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of %s must be a scalar field name, got: %s", functionName, argText)
	}
	dataType := columnInfo.GetDataType()
	if typeutil.IsJSONType(dataType) || len(columnInfo.GetNestedPath()) != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"%s is not supported on JSON or dynamic fields, got: %s", functionName, argText)
	}
	if !typeutil.IsIntegerType(dataType) {
		return merr.WrapErrParameterInvalidMsg(
			"%s only supports INT8/INT16/INT32/INT64 fields, but field (%s) is of type %s",
			functionName, argText, dataType.String())
	}
	return nil
}

// validateRoaringBitmapBlob gates and structurally validates a client pre-built
// MRB1 blob before it is embedded into the plan.
//
// The body is fully validated here, not merely bounded. A Roaring body is a
// nested structure of container descriptors, offsets and run intervals, so
// unlike an SBBF body — which is an opaque bit array — a malformed one can drive
// out-of-range reads or quadratic work in the decoder. roaringfilter.Validate
// walks it without materializing a bitmap, in time linear in the supplied bytes,
// and rejects it here, at the proxy,
// rather than letting every QueryNode discover the problem after fan-out.
func validateRoaringBitmapBlob(blob []byte) (validatedRoaringBitmapBlob, error) {
	// Per-blob gate, mirroring bloom_match's: proxy.maxMembershipFilterSize budgets
	// the body and the fixed MRB1 header (roaringfilter.HeaderSize) is allowed on
	// top. Checked before Validate so an oversized blob is rejected without
	// decoding it.
	//
	// A separate proxy.maxMembershipFilterPlanSize budget bounds the aggregate
	// serialized plans before proto.Marshal. This per-blob gate rejects one
	// oversized bitmap before decode; the aggregate gate limits repeated
	// otherwise-valid occurrences across the request.
	if maxSize := paramtable.Get().ProxyCfg.MaxMembershipFilterSize.GetAsInt(); len(blob) > maxSize+roaringfilter.HeaderSize {
		bodySize := len(blob) - roaringfilter.HeaderSize
		if bodySize < 0 {
			bodySize = 0
		}
		return validatedRoaringBitmapBlob{}, merr.WrapErrParameterInvalidMsg(
			"membership_match roaring bitmap blob body is %d bytes, exceeding proxy.maxMembershipFilterSize (%d); "+
				"a Roaring bitmap's size follows the value distribution, so a sparser member set costs "+
				"more per member than a dense one", bodySize, maxSize)
	}
	summary, err := roaringfilter.Validate(blob)
	if err != nil {
		return validatedRoaringBitmapBlob{}, merr.Wrap(err, "membership_match roaring bitmap blob is invalid")
	}
	return validatedRoaringBitmapBlob{blob: blob, summary: summary}, nil
}
