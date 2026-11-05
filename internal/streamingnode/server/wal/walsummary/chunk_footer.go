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

package walsummary

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// codecVersion is the on-disk chunk format version. It is written once, into
	// the object's fixed binary header, and checked there before anything else
	// is parsed.
	codecVersion      = 1
	chunkHeaderSize   = 16
	chunkChecksumSize = sha256.Size
)

var (
	chunkHeaderMagic = []byte("PSCCH001")
	chunkFooterMagic = []byte("PSCFT001")
)

// chunkValidation is immutable proof that this footer and object size matched
// one retained, immutable manifest index. It must not be reused for another index.
// It outlives payload eviction but is discarded when that index is retired.
type chunkValidation struct {
	digest     [sha256.Size]byte
	objectSize uint64
}

// chunkPayload is immutable. Section decoders return independent records.
type chunkPayload struct {
	bytes       []byte
	footerStart uint64
	verified    *chunkValidation
}

// validateChunkTail checks framing and hashes the footer on EVERY object read,
// including reloads whose footer has previously passed semantic validation.
func validateChunkTail(payload []byte) ([]byte, uint64, [sha256.Size]byte, error) {
	if len(payload) < chunkHeaderSize+chunkChecksumSize+len(chunkFooterMagic)+4 {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("summary chunk payload too short")
	}
	if !bytes.Equal(payload[:len(chunkHeaderMagic)], chunkHeaderMagic) {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("invalid summary chunk header magic")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != codecVersion {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("unsupported summary chunk version %d", version)
	}
	if headerSize := binary.BigEndian.Uint32(payload[12:16]); headerSize != chunkHeaderSize {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("invalid summary chunk header size %d", headerSize)
	}
	footerMagicStart := len(payload) - len(chunkFooterMagic)
	if !bytes.Equal(payload[footerMagicStart:], chunkFooterMagic) {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("invalid summary chunk footer magic")
	}
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - chunkChecksumSize
	if footerChecksumStart < chunkHeaderSize {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("invalid summary chunk footer length offset")
	}
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	if footerLen <= 0 || footerStart < chunkHeaderSize {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("invalid summary chunk footer length")
	}
	footerPayload := payload[footerStart:footerChecksumStart]
	actual := sha256.Sum256(footerPayload)
	if !bytes.Equal(payload[footerChecksumStart:footerLenStart], actual[:]) {
		return nil, 0, [sha256.Size]byte{}, storeCorruptedf("summary chunk footer checksum mismatch")
	}
	return footerPayload, uint64(footerStart), actual, nil
}

func decodeChunkFooter(payload []byte) (*streamingpb.PChannelSummaryChunkFooter, error) {
	footer := &streamingpb.PChannelSummaryChunkFooter{}
	if err := proto.Unmarshal(payload, footer); err != nil {
		return nil, markStoreCorrupted(merr.Wrap(err, "failed to decode summary chunk footer"))
	}
	return footer, nil
}

// unmarshalChunkTail returns the footer and the upper bound for section offsets.
func unmarshalChunkTail(payload []byte) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
	encoded, start, _, err := validateChunkTail(payload)
	if err != nil {
		return nil, 0, err
	}
	footer, err := decodeChunkFooter(encoded)
	if err != nil {
		return nil, 0, err
	}
	return footer, start, nil
}

func validateChunkPayload(payload []byte, pchannel string, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
	encoded, start, digest, err := validateChunkTail(payload)
	if err != nil {
		return nil, err
	}
	size := uint64(len(payload))
	if verified == nil || verified.objectSize != size || verified.digest != digest {
		footer, err := decodeChunkFooter(encoded)
		if err != nil {
			return nil, err
		}
		if footer.GetPchannel() != pchannel || !proto.Equal(chunkIndexEntryFromFooter(footer, size), index) {
			return nil, storeCorruptedf("summary chunk differs from retained index")
		}
		// Different protobuf encodings can describe the same index. Accept and
		// remember them only after the full semantic check succeeds.
		verified = &chunkValidation{digest: digest, objectSize: size}
	}
	return &chunkPayload{bytes: payload, footerStart: start, verified: verified}, nil
}

// chunkIndexEntryFromFooter mirrors a written chunk's footer into a manifest
// entry, so retention and lazy reads work off the manifest alone.
func chunkIndexEntryFromFooter(footer *streamingpb.PChannelSummaryChunkFooter, objectSize uint64) *streamingpb.PChannelSummaryChunkIndexEntry {
	if footer == nil {
		return nil
	}
	return &streamingpb.PChannelSummaryChunkIndexEntry{
		Generation:    footer.GetGeneration(),
		Term:          footer.GetTerm(),
		ObjectSize:    objectSize,
		StartTimeTick: footer.GetStartTimeTick(),
		EndTimetick:   footer.GetEndTimetick(),
		Vchannels:     footer.GetChunks(),
	}
}
