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

package inspector

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
	"testing"
)

func TestInspectV3ValidatesEnvelopeAndRanges(t *testing.T) {
	raw := testV3Object(17, false)
	if err := InspectV3(raw, 17); err != nil {
		t.Fatalf("InspectV3() returned an error for a valid object: %v", err)
	}
}

func TestInspectV3RejectsOverlappingRanges(t *testing.T) {
	raw := testV3Object(17, true)
	if err := InspectV3(raw, 17); err == nil {
		t.Fatal("InspectV3() accepted overlapping ranges")
	}
}

func testV3Object(ezID int64, overlap bool) []byte {
	data := []byte("data-meta")
	secondOffset := uint64(4)
	if overlap {
		secondOffset = 3
	}
	directory, err := json.Marshal(v3Directory{
		SliceSize: 16,
		EDEK:      "fixture-edek",
		EZID:      strconv.FormatInt(ezID, 10),
		Entries: []v3Entry{
			{Name: "index", OriginalSize: 4, Slices: []v3Slice{{Offset: 0, Size: 4}}},
			{Name: ".lock", OriginalSize: 0, Slices: nil},
			{Name: "__meta__", OriginalSize: 5, Slices: []v3Slice{{Offset: secondOffset, Size: 5}}},
		},
	})
	if err != nil {
		panic(err)
	}
	footer := make([]byte, v3FooterSize)
	binary.LittleEndian.PutUint16(footer, v3FormatValue)
	binary.LittleEndian.PutUint32(footer[24:28], 5)
	binary.LittleEndian.PutUint32(footer[28:32], uint32(len(directory)))
	result := append([]byte(v3Magic), data...)
	result = append(result, directory...)
	result = append(result, footer...)
	return result
}
