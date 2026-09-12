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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// The V3 layout is specified in
// docs/design-docs/design_docs/20260209-scalar-index-unified-format.md and is
// implemented by internal/core/src/storage/IndexEntryWriter.h and
// IndexEntryReader.cpp. Keep this test decoder independent from the production
// reader so format regressions cannot make both the implementation and oracle
// pass together.
const (
	v3Magic       = "MVSIDXV3"
	v3FooterSize  = 32
	v3MagicSize   = 8
	v3FormatValue = 3
)

type v3Directory struct {
	SliceSize uint64    `json:"slice_size"`
	Entries   []v3Entry `json:"entries"`
	EDEK      string    `json:"__edek__"`
	EZID      string    `json:"__ez_id__"`
}

type v3Entry struct {
	Name         string    `json:"name"`
	OriginalSize uint64    `json:"original_size"`
	Slices       []v3Slice `json:"slices"`
}

type v3Slice struct {
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size"`
}

type v3Range struct {
	Start uint64
	End   uint64
}

func InspectV3(raw []byte, expectedEZID int64) error {
	if len(raw) < v3MagicSize+v3FooterSize {
		return fmt.Errorf("V3 object is too small")
	}
	if !bytes.Equal(raw[:v3MagicSize], []byte(v3Magic)) {
		return fmt.Errorf("V3 object has invalid magic")
	}

	footer := raw[len(raw)-v3FooterSize:]
	if version := binary.LittleEndian.Uint16(footer); version != v3FormatValue {
		return fmt.Errorf("V3 object format version %d, want %d", version, v3FormatValue)
	}
	metaSize := uint64(binary.LittleEndian.Uint32(footer[24:28]))
	directorySize := uint64(binary.LittleEndian.Uint32(footer[28:32]))
	if directorySize == 0 || directorySize > uint64(len(raw)-v3MagicSize-v3FooterSize) {
		return fmt.Errorf("invalid V3 directory size %d", directorySize)
	}
	directoryStart := uint64(len(raw)) - v3FooterSize - directorySize
	dataRegionSize := directoryStart - v3MagicSize
	if metaSize == 0 || metaSize > dataRegionSize {
		return fmt.Errorf("invalid V3 metadata size %d", metaSize)
	}

	var directory v3Directory
	if err := json.Unmarshal(raw[directoryStart:uint64(len(raw))-v3FooterSize], &directory); err != nil {
		return fmt.Errorf("parse V3 directory: %w", err)
	}
	if directory.EDEK == "" {
		return fmt.Errorf("V3 directory has no EDEK")
	}
	actualEZID, err := strconv.ParseInt(directory.EZID, 10, 64)
	if err != nil || actualEZID != expectedEZID {
		return fmt.Errorf("V3 directory EZ id %q, want %d", directory.EZID, expectedEZID)
	}
	if directory.SliceSize == 0 || len(directory.Entries) == 0 {
		return fmt.Errorf("V3 directory has no slice metadata")
	}

	var ranges []v3Range
	metaCipherSize := uint64(0)
	for _, entry := range directory.Entries {
		if entry.Name == "" {
			return fmt.Errorf("V3 directory contains an incomplete entry")
		}
		// Tantivy persists zero-byte lock entries in its index directory. They
		// are valid envelope entries and intentionally have no slices.
		if entry.OriginalSize == 0 {
			if len(entry.Slices) != 0 {
				return fmt.Errorf("V3 entry %q has slices for an empty entry", entry.Name)
			}
			continue
		}
		if len(entry.Slices) == 0 {
			return fmt.Errorf("V3 directory contains an incomplete entry")
		}
		entryCipherSize := uint64(0)
		for _, slice := range entry.Slices {
			if slice.Size == 0 || slice.Offset > dataRegionSize || slice.Size > dataRegionSize-slice.Offset {
				return fmt.Errorf("V3 entry %q has an out-of-bounds slice", entry.Name)
			}
			ranges = append(ranges, v3Range{Start: slice.Offset, End: slice.Offset + slice.Size})
			entryCipherSize += slice.Size
		}
		if entry.Name == "__meta__" {
			metaCipherSize = entryCipherSize
		}
	}
	if metaCipherSize != metaSize {
		return fmt.Errorf("V3 footer metadata size %d, directory metadata size %d", metaSize, metaCipherSize)
	}

	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].Start < ranges[j].Start
	})
	for i := 1; i < len(ranges); i++ {
		if ranges[i-1].End > ranges[i].Start {
			return fmt.Errorf("V3 directory contains overlapping slices")
		}
	}
	return nil
}
