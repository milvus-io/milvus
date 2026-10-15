package function

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeAnalyzerNames(t *testing.T) {
	for _, tc := range []struct {
		names    []string
		texts    int
		expected []string
	}{
		{nil, 0, []string{}},
		{nil, 2, []string{"default", "default"}},
		{[]string{""}, 2, []string{"default", "default"}},
		{[]string{"en"}, 2, []string{"en", "en"}},
		{[]string{"en", ""}, 2, []string{"en", "default"}},
	} {
		before := append([]string(nil), tc.names...)
		result, err := NormalizeAnalyzerNames(tc.names, tc.texts)
		require.NoError(t, err)
		require.Equal(t, tc.expected, result)
		require.Equal(t, before, tc.names)
	}
	_, err := NormalizeAnalyzerNames([]string{"a", "b", "c"}, 2)
	require.Error(t, err)
}
