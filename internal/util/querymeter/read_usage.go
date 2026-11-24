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

package querymeter

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// AddColumn records an entire planned column. Keep the validity positions
// unrounded until the request is combined so segment boundaries do not change
// its size.
func AddColumn(usage *internalpb.QueryReadUsage, fieldID int64, dataBytes, validityBits uint64) error {
	if err := AddDataBytes(usage, dataBytes); err != nil {
		return err
	}
	return addValidityBits(usage, fieldID, validityBits)
}

// AddReadUsage combines the data-plane counters from one complete contribution.
func AddReadUsage(total, part *internalpb.QueryReadUsage) error {
	if part == nil {
		return merr.WrapErrServiceInternalMsg("query execution did not report read usage")
	}
	if err := AddDataBytes(total, part.GetDataBytes()); err != nil {
		return err
	}
	for fieldID, bits := range part.GetValidityBits() {
		if err := addValidityBits(total, fieldID, bits); err != nil {
			return err
		}
	}
	return nil
}

// AddDataBytes adds a measured contribution without separate validity bits.
func AddDataBytes(usage *internalpb.QueryReadUsage, bytes uint64) error {
	if math.MaxUint64-usage.DataBytes < bytes {
		return merr.WrapErrServiceInternalMsg("query read usage exceeds the byte counter")
	}
	usage.DataBytes += bytes
	return nil
}

func addValidityBits(usage *internalpb.QueryReadUsage, fieldID int64, bits uint64) error {
	if bits == 0 {
		return nil
	}
	if math.MaxUint64-usage.ValidityBits[fieldID] < bits {
		return merr.WrapErrServiceInternalMsg("query read usage exceeds the validity counter for field %d", fieldID)
	}
	if usage.ValidityBits == nil {
		usage.ValidityBits = make(map[int64]uint64)
	}
	usage.ValidityBits[fieldID] += bits
	return nil
}

// BillableBytes produces the single Proxy report value for a successful Query.
func BillableBytes(usage *internalpb.QueryReadUsage, returnedBytes uint64) (int64, error) {
	if usage == nil {
		return 0, merr.WrapErrServiceInternalMsg("query execution did not report read usage")
	}
	total := usage.GetDataBytes()
	if math.MaxInt64 < total || math.MaxInt64-total < returnedBytes {
		return 0, merr.WrapErrServiceInternalMsg("query usage exceeds the report byte counter")
	}
	total += returnedBytes
	for _, bits := range usage.GetValidityBits() {
		bytes := bits / 8
		if bits%8 != 0 {
			bytes++
		}
		if math.MaxInt64-total < bytes {
			return 0, merr.WrapErrServiceInternalMsg("query usage exceeds the report byte counter")
		}
		total += bytes
	}
	return int64(total), nil
}
