package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFieldNameKeywordRejectsNullCaseInsensitive(t *testing.T) {
	for _, name := range []string{"null", "Null", "NULL", "nUlL", "NuLL"} {
		assert.True(t, IsFieldNameKeyword(name), name)
	}
}

func TestIsFieldNameKeywordCoreOperators(t *testing.T) {
	for _, keyword := range []string{"like", "and", "or", "not", "in", "null"} {
		// Enumerate every ASCII casing so title case and mixed case cannot
		// bypass the policy while lowercase and uppercase are blocked.
		for mask := 0; mask < 1<<len(keyword); mask++ {
			name := []byte(keyword)
			for i := range name {
				if mask&(1<<i) != 0 {
					name[i] -= 'a' - 'A'
				}
			}
			t.Run(string(name), func(t *testing.T) {
				assert.True(t, IsFieldNameKeyword(string(name)))
			})
		}
	}
}

func TestIsFieldNameKeywordContextualWords(t *testing.T) {
	for _, keyword := range []string{
		"text_match_fuzzy", "match_all", "match_any", "match_least", "match_most", "match_exact",
		"iso", "interval", "minimum_should_match", "threshold", "element_filter",
		"st_equals", "st_touches", "st_overlaps", "st_crosses", "st_contains",
		"st_intersects", "st_within", "st_dwithin", "st_isvalid",
	} {
		for _, name := range []string{keyword, strings.ToUpper(keyword)} {
			t.Run(name, func(t *testing.T) {
				assert.False(t, IsFieldNameKeyword(name))
			})
		}
	}
}

func TestIsFieldNameKeywordExistingPolicy(t *testing.T) {
	for name := range FieldNameKeywords {
		assert.True(t, IsFieldNameKeyword(name), name)
	}
	// Unrelated operators and function names keep their existing exact-case
	// policy. Soft option names and names merely containing a keyword stay valid.
	for _, name := range []string{
		"Exists", "Text_Match", "Phrase_Match", "Random_Sample", "Array_Length", "Json_Contains",
		"max_edit_distance", "membership_match", "type", "bloom", "roaring",
		"null_value", "NOTES", "like_count", "in_stock",
	} {
		assert.False(t, IsFieldNameKeyword(name), name)
	}
}
