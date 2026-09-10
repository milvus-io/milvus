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

package codec

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// valueKind is the first byte of a stored value and selects its layout:
//
//	| kind | name               | layout                  | meaning       |
//	|------|--------------------|-------------------------|---------------|
//	| 0x00 | valueKindTombstone | [0x00]                  | PK is deleted |
//	| 0x01 | valueKindPKEntry   | [0x01][SegmentID 8B BE] | PK's live row |
//
// These layouts are hand-encoded, not protobuf, because they sit on the probe
// path; value_test.go pins their exact bytes. Decoding a kind 0x01 value reads
// SegmentID at a fixed offset, with no allocation and no protobuf.
//
// Deletes are stored as valueKindTombstone values rather than as engine-native
// tombstones, so that a recent delete in the increment layer masks an older
// entry in the read-only baseline layer, and survives a memtable flush into
// an SST. Merge (LWW) physically drops tombstoned keys.
//
// TODO: optional fields go into a protobuf message appended after the fixed
// part, so the common kind stays hand-encoded and an old reader skips fields
// it does not know:
//
//	| kind | name                | layout                              |
//	|------|---------------------|-------------------------------------|
//	| 0x02 | valueKindPKEntryExt | [0x02][SegmentID 8B BE][PKEntryExt] |
//
// with PKEntryExt defined in pkg/proto/pkindex.proto as:
//
//	message PKEntryExt {
//	  uint64 custom_ts  = 1;
//	  bytes  field_data = 2;
//	}
//
// PKEntryExt runs to the end of the value, so it needs no length prefix.
// SegmentID sits at the same offset in kinds 0x01 and 0x02, so a caller that
// only needs SegmentID never unmarshals PKEntryExt; only a caller that reads
// the optional fields pays for protobuf decoding.
//
// A tombstone carries no fields while LWW follows WAL order. If CustomTs ever
// decides LWW, a delete must carry it too, which takes a new tombstone kind
// with the same PKEntryExt tail.
type valueKind byte

const (
	valueKindTombstone valueKind = 0x00
	valueKindPKEntry   valueKind = 0x01
)

// pkEntrySize is the encoded size of a valueKindPKEntry value.
const pkEntrySize = 1 + 8

// PKEntry is the value stored per primary key: the segment currently holding
// the live row for that PK.
type PKEntry struct {
	SegmentID int64
}

// EncodePKEntry encodes a PKEntry as a valueKindPKEntry value: the kind byte
// followed by the 8-byte big-endian segment ID.
func EncodePKEntry(e PKEntry) []byte {
	b := make([]byte, pkEntrySize)
	b[0] = byte(valueKindPKEntry)
	binary.BigEndian.PutUint64(b[1:], uint64(e.SegmentID))
	return b
}

// EncodeTombstone returns the value marking a deleted PK.
func EncodeTombstone() []byte {
	return []byte{byte(valueKindTombstone)}
}

// IsTombstone reports whether a stored value is the delete marker.
func IsTombstone(b []byte) bool {
	return len(b) == 1 && valueKind(b[0]) == valueKindTombstone
}

// DecodePKEntry decodes a value produced by EncodePKEntry.
func DecodePKEntry(b []byte) (PKEntry, error) {
	if len(b) < pkEntrySize {
		return PKEntry{}, errors.Mark(
			errors.Newf("invalid pk entry length %d, want at least %d", len(b), pkEntrySize),
			pkerr.ErrCorrupted)
	}
	if valueKind(b[0]) != valueKindPKEntry {
		return PKEntry{}, errors.Mark(
			errors.Newf("unknown pk value kind %d", b[0]),
			pkerr.ErrCorrupted)
	}
	return PKEntry{SegmentID: int64(binary.BigEndian.Uint64(b[1:pkEntrySize]))}, nil
}
