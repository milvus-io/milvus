package snapshotio

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSegmentManifestBoundsOCFMetadataMaps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first int64
		next  int64
	}{
		{name: "single oversized block", next: 100},
		{name: "cumulative chunked blocks", first: 6, next: 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(ocfMagic)
			if tc.first > 0 {
				data = binary.AppendVarint(data, tc.first)
				for range tc.first {
					data = append(data, 0, 0) // Empty metadata key and bytes value.
				}
			}
			data = binary.AppendVarint(data, tc.next)
			_, err := ParseSegmentManifest(data, SelfDescribingManifestVersion)
			require.ErrorContains(t, err, "Config.MaxMapAllocSize")
		})
	}
}
