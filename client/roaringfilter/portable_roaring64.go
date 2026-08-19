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

package roaringfilter

import (
	"encoding/binary"
	"math/bits"

	"github.com/cockroachdb/errors"
)

const (
	portableCookieNoRun         uint32 = 12346
	portableCookieRun           uint16 = 12347
	portableArrayMaxCardinality        = 4096
	portableBitmapBytes                = 8192
	portableNoOffsetThreshold          = 4
)

func portableBodyError(format string, args ...any) error {
	return errors.Errorf("invalid portable roaring64 body: "+format, args...)
}

func requirePortableBytes(data []byte, cursor, size int, what string) error {
	if cursor < 0 || size < 0 || cursor > len(data)-size {
		return portableBodyError("truncated %s", what)
	}
	return nil
}

type portableValidationSummary struct {
	consumed           int
	cardinality        uint64
	highContainerCount uint64
	lowContainerCount  uint64
}

// validatePortableRoaring64Body validates the portable format in time linear
// in the supplied bytes. We cannot call roaring.Bitmap.Validate on untrusted
// input: roaring/v2 validates each run interval against every later interval,
// which makes a small, attacker-controlled run container quadratic in CPU.
func validatePortableRoaring64Body(body []byte) error {
	_, err := scanPortableRoaring64Body(body)
	return err
}

// scanPortableRoaring64Body validates the portable format and returns the
// resource-bearing counts needed for admission. The success path performs no
// heap allocation: it only reads the caller-owned byte slice.
func scanPortableRoaring64Body(body []byte) (portableValidationSummary, error) {
	if err := prevalidatePortableRoaring64Body(body); err != nil {
		return portableValidationSummary{}, err
	}

	highContainerCount := binary.LittleEndian.Uint64(body[:portableRoaring64PrefixBytes])
	cursor := portableRoaring64PrefixBytes
	var previousHigh uint32
	var cardinality uint64
	var lowContainerCount uint64
	for i := uint64(0); i < highContainerCount; i++ {
		if err := requirePortableBytes(body, cursor, 4, "high key"); err != nil {
			return portableValidationSummary{}, err
		}
		high := binary.LittleEndian.Uint32(body[cursor : cursor+4])
		if i > 0 && high <= previousHigh {
			return portableValidationSummary{}, portableBodyError(
				"high container %d has a non-increasing key", i)
		}
		previousHigh = high
		cursor += 4

		consumed, childCardinality, childContainerCount, err := validatePortableRoaring32(body[cursor:])
		if err != nil {
			return portableValidationSummary{}, err
		}
		if childCardinality > ^uint64(0)-cardinality {
			return portableValidationSummary{}, portableBodyError("Roaring64 cardinality overflows")
		}
		cardinality += childCardinality
		if childContainerCount > ^uint64(0)-lowContainerCount {
			return portableValidationSummary{}, portableBodyError("low-container count overflows")
		}
		lowContainerCount += childContainerCount
		cursor += consumed
	}
	if cursor != len(body) {
		return portableValidationSummary{}, portableBodyError("consumed %d bytes, expected %d", cursor, len(body))
	}
	return portableValidationSummary{
		consumed:           cursor,
		cardinality:        cardinality,
		highContainerCount: highContainerCount,
		lowContainerCount:  lowContainerCount,
	}, nil
}

func validatePortableRoaring32(data []byte) (int, uint64, uint64, error) {
	if err := requirePortableBytes(data, 0, 4, "Roaring32 cookie"); err != nil {
		return 0, 0, 0, err
	}
	cookie := binary.LittleEndian.Uint32(data[:4])
	cursor := 4
	var (
		containerCount uint32
		runBitmap      []byte
	)

	if uint16(cookie) == portableCookieRun {
		containerCount = (cookie >> 16) + 1
		runBitmapBytes := (int(containerCount) + 7) / 8
		if err := requirePortableBytes(data, cursor, runBitmapBytes, "run bitmap"); err != nil {
			return 0, 0, 0, err
		}
		runBitmap = data[cursor : cursor+runBitmapBytes]
		cursor += runBitmapBytes
		// The portable format sizes the run bitmap at (containerCount + 7) / 8
		// bytes and assigns meaning only to the first containerCount bits. The
		// remaining bits are unspecified padding: roaring/v2 and CRoaring both
		// ignore them, so requiring them to be zero would reject bodies those
		// writers can produce.
	} else if cookie == portableCookieNoRun {
		if err := requirePortableBytes(data, cursor, 4, "Roaring32 container count"); err != nil {
			return 0, 0, 0, err
		}
		containerCount = binary.LittleEndian.Uint32(data[cursor : cursor+4])
		cursor += 4
		// A zero-container child is legal: CRoaring writes it for an empty
		// Roaring32 under a high key, and both CRoaring and roaring/v2 decode
		// the result as an empty set.
		if containerCount > 1<<16 {
			return 0, 0, 0, portableBodyError("invalid Roaring32 container count %d", containerCount)
		}
	} else {
		return 0, 0, 0, portableBodyError("unsupported Roaring32 cookie %d", cookie)
	}

	descriptorBytes := int(containerCount) * 4
	if err := requirePortableBytes(data, cursor, descriptorBytes, "container descriptors"); err != nil {
		return 0, 0, 0, err
	}
	descriptorStart := cursor
	var previousKey uint16
	for i := uint32(0); i < containerCount; i++ {
		offset := descriptorStart + int(i)*4
		key := binary.LittleEndian.Uint16(data[offset : offset+2])
		if i > 0 && key <= previousKey {
			return 0, 0, 0, portableBodyError(
				"Roaring32 container %d has a non-increasing key", i)
		}
		previousKey = key
	}
	cursor += descriptorBytes

	// The offset table is skipped, not verified. It is redundant with the
	// container layout, and both roaring/v2 and CRoaring ignore its contents on
	// read, so requiring each entry to equal the position we computed would
	// reject bodies the reference readers accept. Its SIZE still has to be
	// present, which is what keeps the remaining bounds checks honest.
	hasOffsets := runBitmap == nil || containerCount >= portableNoOffsetThreshold
	if hasOffsets {
		offsetBytes := int(containerCount) * 4
		if err := requirePortableBytes(data, cursor, offsetBytes, "container offsets"); err != nil {
			return 0, 0, 0, err
		}
		cursor += offsetBytes
	}

	var cardinalitySum uint64
	for i := uint32(0); i < containerCount; i++ {
		descriptorOffset := descriptorStart + int(i)*4
		cardinality := uint32(binary.LittleEndian.Uint16(
			data[descriptorOffset+2:descriptorOffset+4])) + 1
		isRun := runBitmap != nil && runBitmap[i/8]&(1<<(i%8)) != 0
		var err error
		if isRun {
			cursor, err = validatePortableRunContainer(data, cursor, cardinality)
		} else if cardinality > portableArrayMaxCardinality {
			cursor, err = validatePortableBitmapContainer(data, cursor, cardinality)
		} else {
			cursor, err = validatePortableArrayContainer(data, cursor, cardinality)
		}
		if err != nil {
			return 0, 0, 0, err
		}
		cardinalitySum += uint64(cardinality)
	}
	return cursor, cardinalitySum, uint64(containerCount), nil
}

func validatePortableArrayContainer(data []byte, cursor int, cardinality uint32) (int, error) {
	bytesNeeded := int(cardinality) * 2
	if err := requirePortableBytes(data, cursor, bytesNeeded, "array container"); err != nil {
		return 0, err
	}
	var previous uint16
	for i := uint32(0); i < cardinality; i++ {
		value := binary.LittleEndian.Uint16(data[cursor+int(i)*2 : cursor+int(i+1)*2])
		if i > 0 && value <= previous {
			return 0, portableBodyError("array values must be strictly increasing")
		}
		previous = value
	}
	return cursor + bytesNeeded, nil
}

func validatePortableBitmapContainer(data []byte, cursor int, cardinality uint32) (int, error) {
	if err := requirePortableBytes(data, cursor, portableBitmapBytes, "bitmap container"); err != nil {
		return 0, err
	}
	actualCardinality := 0
	for offset := cursor; offset < cursor+portableBitmapBytes; offset += 8 {
		actualCardinality += bits.OnesCount64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	}
	if uint32(actualCardinality) != cardinality {
		return 0, portableBodyError(
			"bitmap cardinality %d does not match descriptor %d", actualCardinality, cardinality)
	}
	return cursor + portableBitmapBytes, nil
}

func validatePortableRunContainer(data []byte, cursor int, cardinality uint32) (int, error) {
	if err := requirePortableBytes(data, cursor, 2, "run count"); err != nil {
		return 0, err
	}
	// A run count of zero cannot be consistent with any descriptor: the
	// descriptor stores cardinality - 1, so it cannot express an empty
	// container, and a zero-run body is rejected downstream on the cardinality
	// mismatch regardless. Checking here only makes the error say what is
	// actually wrong.
	runCount := uint32(binary.LittleEndian.Uint16(data[cursor : cursor+2]))
	if runCount == 0 {
		return 0, portableBodyError("invalid run count %d", runCount)
	}
	cursor += 2
	runBytes := int(runCount) * 4
	if err := requirePortableBytes(data, cursor, runBytes, "run intervals"); err != nil {
		return 0, err
	}

	var (
		previousEnd    uint32
		cardinalitySum uint32
	)
	for i := uint32(0); i < runCount; i++ {
		offset := cursor + int(i)*4
		start := uint32(binary.LittleEndian.Uint16(data[offset : offset+2]))
		length := uint32(binary.LittleEndian.Uint16(data[offset+2 : offset+4]))
		end := start + length
		if end > 0xffff {
			return 0, portableBodyError("run interval exceeds uint16 range")
		}
		// The portable format requires runs to be sorted and non-overlapping,
		// NOT merged: [0,0] followed by [1,1] is a legal encoding of {0,1} and
		// both roaring/v2 and CRoaring decode it that way. Reject only actual
		// overlap or descending order.
		if i > 0 && start <= previousEnd {
			return 0, portableBodyError("run intervals overlap or are out of order")
		}
		previousEnd = end
		if cardinalitySum > 1<<16-(length+1) {
			return 0, portableBodyError("run cardinality exceeds container capacity")
		}
		cardinalitySum += length + 1
	}
	if cardinalitySum != cardinality {
		return 0, portableBodyError(
			"run cardinality %d does not match descriptor %d", cardinalitySum, cardinality)
	}
	return cursor + runBytes, nil
}
