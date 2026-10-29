package walsummary

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func footerTestObject(encoded []byte) []byte {
	payload := make([]byte, chunkHeaderSize)
	copy(payload, chunkHeaderMagic)
	binary.BigEndian.PutUint16(payload[8:10], codecVersion)
	binary.BigEndian.PutUint32(payload[12:16], chunkHeaderSize)
	payload = append(payload, encoded...)
	digest := sha256.Sum256(encoded)
	payload = append(payload, digest[:]...)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(encoded)))
	return append(payload, chunkFooterMagic...)
}

func footerTestFixture(t testing.TB) ([]byte, *streamingpb.PChannelSummaryChunkIndexEntry) {
	t.Helper()
	footer := &streamingpb.PChannelSummaryChunkFooter{
		Pchannel: "p1", Generation: 1, Term: 1, StartTimeTick: 1, EndTimetick: 100,
		Chunks: []*streamingpb.VChannelSummaryChunkIndex{{Vchannel: "v1", StartTimetick: 1, EndTimetick: 100}},
	}
	encoded, err := proto.Marshal(footer)
	require.NoError(t, err)
	payload := footerTestObject(encoded)
	return payload, chunkIndexEntryFromFooter(footer, uint64(len(payload)))
}

func TestChunkFooterReusesValidation(t *testing.T) {
	payload, index := footerTestFixture(t)
	var calls int
	var original func([]byte) (*streamingpb.PChannelSummaryChunkFooter, error)
	patch := mockey.Mock(decodeChunkFooter).Origin(&original).To(func(b []byte) (*streamingpb.PChannelSummaryChunkFooter, error) {
		calls++
		return original(b)
	}).Build()
	defer patch.UnPatch()
	first, err := validateChunkPayload(payload, "p1", index, nil)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	for range 3 {
		next, err := validateChunkPayload(payload, "p1", index, first.verified)
		require.NoError(t, err)
		require.Same(t, first.verified, next.verified)
	}
	require.Equal(t, 1, calls, "unchanged footer must not be decoded again")

	// Reorder top-level wire fields without changing semantic content or size.
	encoded, _, _, err := validateChunkTail(payload)
	require.NoError(t, err)
	var fields [][]byte
	for len(encoded) > 0 {
		_, _, n := protowire.ConsumeField(encoded)
		require.Positive(t, n)
		fields = append(fields, encoded[:n])
		encoded = encoded[n:]
	}
	var reordered []byte
	for i := len(fields) - 1; i >= 0; i-- {
		reordered = append(reordered, fields[i]...)
	}
	different := footerTestObject(reordered)
	require.NotEqual(t, payload, different)
	second, err := validateChunkPayload(different, "p1", index, first.verified)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "different encoding must pass semantic validation")
	require.NotEqual(t, first.verified.digest, second.verified.digest)
	third, err := validateChunkPayload(different, "p1", index, second.verified)
	require.NoError(t, err)
	require.Same(t, second.verified, third.verified)
	require.Equal(t, 2, calls)
}

func TestChunkFooterRejectsDamageAfterValidation(t *testing.T) {
	payload, index := footerTestFixture(t)
	first, err := validateChunkPayload(payload, "p1", index, nil)
	require.NoError(t, err)
	cases := map[string]func([]byte) []byte{
		"short":              func(b []byte) []byte { return b[:chunkHeaderSize] },
		"header":             func(b []byte) []byte { b[0] ^= 1; return b },
		"version":            func(b []byte) []byte { b[9]++; return b },
		"header size":        func(b []byte) []byte { b[15]++; return b },
		"footer magic":       func(b []byte) []byte { b[len(b)-1] ^= 1; return b },
		"footer length":      func(b []byte) []byte { binary.BigEndian.PutUint32(b[len(b)-12:], ^uint32(0)); return b },
		"zero footer length": func(b []byte) []byte { binary.BigEndian.PutUint32(b[len(b)-12:], 0); return b },
		"footer bytes":       func(b []byte) []byte { b[chunkHeaderSize] ^= 1; return b },
		"checksum":           func(b []byte) []byte { b[len(b)-13] ^= 1; return b },
		// Same footer/checksum, but a different object length must not hit the shortcut.
		"object size": func(b []byte) []byte {
			return append(append(b[:chunkHeaderSize:chunkHeaderSize], 0), b[chunkHeaderSize:]...)
		},
		"malformed protobuf": func([]byte) []byte { return footerTestObject([]byte{0xff}) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			damaged := mutate(append([]byte(nil), payload...))
			for _, verified := range []*chunkValidation{nil, first.verified} {
				_, err := validateChunkPayload(damaged, "p1", index, verified)
				require.ErrorIs(t, err, ErrStoreCorrupted)
			}
		})
	}
	for _, field := range []string{"pchannel", "generation", "term", "vchannel", "range"} {
		t.Run(field, func(t *testing.T) {
			footer, _, err := unmarshalChunkTail(payload)
			require.NoError(t, err)
			switch field {
			case "pchannel":
				footer.Pchannel = "p2"
			case "generation":
				footer.Generation++
			case "term":
				footer.Term++
			case "vchannel":
				footer.Chunks[0].Vchannel = "v2"
			case "range":
				footer.EndTimetick++
			}
			encoded, err := proto.Marshal(footer)
			require.NoError(t, err)
			_, err = validateChunkPayload(footerTestObject(encoded), "p1", index, first.verified)
			require.ErrorIs(t, err, ErrStoreCorrupted, "a valid new checksum is not proof of index equivalence")
		})
	}
}

func TestChunkFooterUnmarshalErrors(t *testing.T) {
	for _, payload := range [][]byte{nil, footerTestObject([]byte{0xff})} {
		_, _, err := unmarshalChunkTail(payload)
		require.ErrorIs(t, err, ErrStoreCorrupted)
	}
	require.Nil(t, chunkIndexEntryFromFooter(nil, 0))
}

func BenchmarkChunkFooterValidation(b *testing.B) {
	for _, channels := range []int{100, 1000, 10000} {
		footer := &streamingpb.PChannelSummaryChunkFooter{Pchannel: "p1", Generation: 1, Term: 1}
		for i := range channels {
			footer.Chunks = append(footer.Chunks, &streamingpb.VChannelSummaryChunkIndex{Vchannel: fmt.Sprintf("collection_%d_v0", i), StartTimetick: 1, EndTimetick: 100, Inserts: &streamingpb.VChannelSummarySectionRef{Offset: uint64(i * 100), Length: 100, RecordCount: 10}})
		}
		encoded, err := proto.Marshal(footer)
		require.NoError(b, err)
		payload := footerTestObject(encoded)
		index := chunkIndexEntryFromFooter(footer, uint64(len(payload)))
		first, err := validateChunkPayload(payload, "p1", index, nil)
		require.NoError(b, err)
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("channels=%d/reuse=%t", channels, reuse), func(b *testing.B) {
				var verified *chunkValidation
				if reuse {
					verified = first.verified
				}
				b.ReportAllocs()
				for range b.N {
					if _, err := validateChunkPayload(payload, "p1", index, verified); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
