package snview

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServingEpochCompatibility(t *testing.T) {
	for _, tc := range []struct {
		request, view uint64
		match         bool
	}{
		{0, 0, true}, {0, 1, true}, {0, 2, false}, {2, 0, false}, {2, 1, false}, {2, 2, true}, {2, 3, false}, {3, 2, false},
	} {
		require.Equal(t, tc.match, matchesServingEpoch(tc.request, tc.view), "request=%d view=%d", tc.request, tc.view)
	}
}
