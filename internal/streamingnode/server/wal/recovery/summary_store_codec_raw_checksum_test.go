package recovery

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// The footer integrity check must cover the exact stored footer bytes, carried
// in the binary trailer — not a checksum embedded in the footer and re-derived
// by re-marshaling the parsed struct. Proto marshaling is not guaranteed
// byte-stable across library versions, so re-deriving would falsely flag a
// healthy chunk as corrupt the day the encoding shifts. We prove the property by
// re-encoding the footer into a byte-different but semantically identical form
// and refreshing the trailer checksum: decode must still accept it.
func TestPChannelSummaryFooterChecksumCoversStoredBytes(t *testing.T) {
	payload, _, err := marshalPChannelSummaryChunk("p1", 3, 0, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-1", 200, 7)},
	})
	require.NoError(t, err)

	rebuilt, _ := repackChunkWithPaddedFooter(t, payload)
	require.NotEqual(t, payload, rebuilt)

	records, footer, err := unmarshalPChannelSummaryChunk(rebuilt)
	require.NoError(t, err)
	require.Len(t, records["v1"], 1)
	require.Equal(t, uint64(3), footer.GetGeneration())
}

// repackChunkWithPaddedFooter re-frames a chunk with an unknown proto field
// appended to its footer and a refreshed trailer checksum. The result is byte-
// different from the input but decodes to the same footer identity, because a
// proto decoder preserves and ignores unknown fields. It returns the rebuilt
// chunk and the new footer checksum.
func repackChunkWithPaddedFooter(t *testing.T, payload []byte) ([]byte, []byte) {
	t.Helper()
	footerMagicStart := len(payload) - len(pchannelSummaryChunkFooterMagic)
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - sha256.Size
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen

	// Field 1000, varint wire type, value 1: unknown to the footer message.
	padded := append([]byte(nil), payload[footerStart:footerChecksumStart]...)
	padded = append(padded, 0xC0, 0x3E, 0x01)
	checksum := sha256.Sum256(padded)

	rebuilt := append([]byte(nil), payload[:footerStart]...)
	rebuilt = append(rebuilt, padded...)
	rebuilt = append(rebuilt, checksum[:]...)
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(padded)))
	rebuilt = append(rebuilt, lenBytes...)
	rebuilt = append(rebuilt, pchannelSummaryChunkFooterMagic...)
	return rebuilt, checksum[:]
}
