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

// Package roaringfilter validates the exact MRB1 membership-filter envelope.
// It is the server-side half of the format: the proxy calls Validate on an
// untrusted client blob before embedding it in a plan. Construction lives in
// the SDK (client/v3/roaringfilter) and decoding lives in segcore
// (internal/core/src/common/RoaringMembership.cpp), so neither is exported
// here.
//
// Its body is the portable 64-bit Roaring format implemented by Go Roaring v2
// and CRoaring C++. Java interoperability is not claimed until independent
// Java fixtures pass the cross-language suite. Signed values preserve their
// int64 two's-complement bits when mapped into the uint64 key space.
package roaringfilter

import (
	"bytes"
	"encoding/binary"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// Magic is the 4-byte MRB1 envelope magic.
	Magic = "MRB1"
	// Version is the MRB1 envelope version implemented by this package.
	Version uint16 = 1
	// FormatPortableRoaring64 identifies the RoaringFormatSpec portable
	// extension for 64-bit integers.
	FormatPortableRoaring64 uint16 = 1
	// HeaderSize is the size in bytes of the MRB1 envelope header.
	HeaderSize = 32
	// MaxBodyBytes bounds an untrusted portable body.
	MaxBodyBytes = 128 * 1024 * 1024
	// MaxHighContainerCount bounds the number of separately allocated Roaring32
	// children created by the C++ execution engine.
	MaxHighContainerCount = 1 << 18
	// MaxEstimatedDecodedBytes bounds one bitmap. The estimate includes the
	// portable body plus conservative fixed overhead for every high-32 child and
	// every Roaring32 container.
	MaxEstimatedDecodedBytes = 64 * 1024 * 1024
	// EstimatedHighContainerOverheadBytes covers the inline C++ Roaring object,
	// its vector entry, and allocator metadata for its top-level arrays.
	EstimatedHighContainerOverheadBytes = 128
	// EstimatedLowContainerOverheadBytes covers decoded container pointers,
	// keys/typecodes, the container header, and allocator metadata. The portable
	// payload itself is already charged through body bytes.
	EstimatedLowContainerOverheadBytes = 64

	portableRoaring64PrefixBytes   = 8
	portableRoaring64MinEntryBytes = 12
)

// ValidationSummary describes a structurally valid MRB1 blob without
// materializing a roaring64.Bitmap.
//
// The proxy discards it today (`_, err := Validate(blob)`). It is kept because
// the counts are what the cross-module contract test grades the validator
// against, and because admitted-blob shape is the natural thing to log or meter
// at the proxy if that is ever wanted.
type ValidationSummary struct {
	Cardinality           uint64
	BodyBytes             uint64
	HighContainerCount    uint64
	LowContainerCount     uint64
	EstimatedDecodedBytes uint64
}

func prevalidatePortableRoaring64Body(body []byte) error {
	if len(body) < portableRoaring64PrefixBytes {
		return merr.WrapErrParameterInvalidMsg(
			"invalid portable roaring64 body: too short for high-container count: %d bytes",
			len(body))
	}

	highContainerCount := binary.LittleEndian.Uint64(body[:portableRoaring64PrefixBytes])
	// Every high container needs a 4-byte high key plus at least an 8-byte
	// portable Roaring32 child: the no-run cookie followed by a container
	// count of zero. CRoaring 3.0.0 emits exactly that for an empty child, and
	// both CRoaring and roaring/v2 consume it, so the bound must admit 12 bytes
	// per entry rather than assuming every child carries a value. This
	// necessary bound prevents ReadFrom from allocating its three top-level
	// slices from an attacker-controlled count that the body cannot possibly
	// contain.
	maxCountFromBody := uint64(len(body)-portableRoaring64PrefixBytes) / portableRoaring64MinEntryBytes
	if highContainerCount > maxCountFromBody {
		return merr.WrapErrParameterInvalidMsg(
			"invalid portable roaring64 body: high-container count %d exceeds body-derived maximum %d",
			highContainerCount, maxCountFromBody)
	}
	if highContainerCount > MaxHighContainerCount {
		return merr.WrapErrParameterInvalidMsg(
			"roaring bitmap high-container count %d exceeds maximum %d",
			highContainerCount, MaxHighContainerCount)
	}
	return nil
}

func validateEnvelope(blob []byte) (uint64, []byte, error) {
	if len(blob) < HeaderSize {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap blob too short: %d bytes, need at least %d", len(blob), HeaderSize)
	}
	if len(blob) > HeaderSize+MaxBodyBytes {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap blob too large: %d bytes, exceeds max %d",
			len(blob), HeaderSize+MaxBodyBytes)
	}
	if !bytes.Equal(blob[0:4], []byte(Magic)) {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap blob has invalid magic, expected %q", Magic)
	}
	if version := binary.LittleEndian.Uint16(blob[4:6]); version != Version {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"unsupported roaring bitmap version %d, expected %d", version, Version)
	}
	if format := binary.LittleEndian.Uint16(blob[6:8]); format != FormatPortableRoaring64 {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"unsupported roaring bitmap format %d, expected %d", format, FormatPortableRoaring64)
	}
	if reserved := binary.LittleEndian.Uint64(blob[24:32]); reserved != 0 {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap reserved field must be 0, got %d", reserved)
	}

	bodyLen := binary.LittleEndian.Uint64(blob[16:24])
	if bodyLen > MaxBodyBytes {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap body too large: %d bytes, exceeds max %d", bodyLen, MaxBodyBytes)
	}
	actualBodyLen := uint64(len(blob) - HeaderSize)
	if bodyLen != actualBodyLen {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap body length %d does not match header value %d", actualBodyLen, bodyLen)
	}

	return binary.LittleEndian.Uint64(blob[8:16]), blob[HeaderSize:], nil
}

func estimateAndCheckDecodedBytes(bodyBytes, highContainerCount, lowContainerCount uint64) (uint64, error) {
	if highContainerCount > MaxHighContainerCount {
		return 0, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap high-container count %d exceeds maximum %d",
			highContainerCount, MaxHighContainerCount)
	}
	estimated := bodyBytes + highContainerCount*EstimatedHighContainerOverheadBytes +
		lowContainerCount*EstimatedLowContainerOverheadBytes
	if estimated > MaxEstimatedDecodedBytes {
		return 0, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap estimated decoded size %d bytes exceeds maximum %d",
			estimated, MaxEstimatedDecodedBytes)
	}
	return estimated, nil
}

// Validate checks an MRB1 blob and reports its resource shape without
// materializing a roaring64.Bitmap. Its success path is allocation-free.
func Validate(blob []byte) (ValidationSummary, error) {
	declaredCardinality, body, err := validateEnvelope(blob)
	if err != nil {
		return ValidationSummary{}, err
	}
	wire, err := scanPortableRoaring64Body(body)
	if err != nil {
		return ValidationSummary{}, err
	}
	if wire.cardinality != declaredCardinality {
		return ValidationSummary{}, merr.WrapErrParameterInvalidMsg(
			"roaring bitmap cardinality %d does not match declared value %d",
			wire.cardinality, declaredCardinality)
	}
	estimatedDecodedBytes, err := estimateAndCheckDecodedBytes(
		uint64(len(body)), wire.highContainerCount, wire.lowContainerCount)
	if err != nil {
		return ValidationSummary{}, err
	}
	return ValidationSummary{
		Cardinality:           wire.cardinality,
		BodyBytes:             uint64(len(body)),
		HighContainerCount:    wire.highContainerCount,
		LowContainerCount:     wire.lowContainerCount,
		EstimatedDecodedBytes: estimatedDecodedBytes,
	}, nil
}
