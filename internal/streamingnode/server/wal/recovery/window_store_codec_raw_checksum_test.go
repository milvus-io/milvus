package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// The footer integrity check must cover the exact stored footer bytes, carried
// in the binary trailer — not a checksum embedded in the JSON and re-derived by
// re-marshaling the parsed struct. Re-deriving would falsely flag corruption the
// day a proto field's JSON encoding changes. We prove the property by re-encoding
// the footer JSON into a byte-different but semantically identical form (indented)
// and refreshing the trailer checksum: decode must still accept it.
func TestPChannelWindowFooterChecksumCoversStoredBytes(t *testing.T) {
	payload, _, checksum, err := marshalPChannelWindowChunk("p1", 3, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, checksum)

	footerMagicStart := len(payload) - len(pchannelWindowChunkFooterMagic)
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - sha256.Size
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen

	var indented bytes.Buffer
	require.NoError(t, json.Indent(&indented, payload[footerStart:footerChecksumStart], "", "  "))
	newFooter := indented.Bytes()
	require.NotEqual(t, payload[footerStart:footerChecksumStart], newFooter)
	newChecksum := sha256.Sum256(newFooter)

	rebuilt := make([]byte, 0, footerStart+len(newFooter)+sha256.Size+4+len(pchannelWindowChunkFooterMagic))
	rebuilt = append(rebuilt, payload[:footerStart]...)
	rebuilt = append(rebuilt, newFooter...)
	rebuilt = append(rebuilt, newChecksum[:]...)
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(newFooter)))
	rebuilt = append(rebuilt, lenBytes...)
	rebuilt = append(rebuilt, pchannelWindowChunkFooterMagic...)

	records, footer, decodedChecksum, err := unmarshalPChannelWindowChunk(rebuilt)
	require.NoError(t, err)
	require.Empty(t, records)
	require.Equal(t, uint64(3), footer.Generation)
	require.Equal(t, hex.EncodeToString(newChecksum[:]), decodedChecksum)
}
