package planparserv2

import (
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

var wildcards = map[byte]struct{}{
	'_': {},
	'%': {},
}

var escapeCharacter byte = '\\'

// hasWildcards returns true if pattern contains any wildcard.
func hasWildcards(pattern string) (string, bool) {
	var result strings.Builder
	hasWildcard := false

	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '\\' && i+1 < len(pattern) {
			next := pattern[i+1]
			if next == '_' || next == '%' {
				result.WriteByte(next)
				i++
				continue
			}
		}

		if pattern[i] == '_' || pattern[i] == '%' {
			hasWildcard = true
		}
		result.WriteByte(pattern[i])
	}

	return result.String(), hasWildcard
}

// findLastNotOfWildcards find the last location not of last wildcard.
func findLastNotOfWildcards(pattern string) int {
	loc := len(pattern) - 1
	for ; loc >= 0; loc-- {
		_, ok := wildcards[pattern[loc]]
		if !ok {
			break
		}
		if ok {
			if loc > 0 && pattern[loc-1] == escapeCharacter {
				break
			}
		}
	}
	return loc
}

// translatePatternMatch translates pattern to related op type and operand.
func translatePatternMatch(pattern string) (op planpb.OpType, operand string, err error) {
	l := len(pattern)
	loc := findLastNotOfWildcards(pattern)

	if loc < 0 {
		// always match.
		return planpb.OpType_PrefixMatch, "", nil
	}

	newPattern, exist := hasWildcards(pattern[:loc+1])
	if loc >= l-1 && !exist {
		// equal match.
		return planpb.OpType_Equal, newPattern, nil
	}
	if !exist {
		// prefix match.
		return planpb.OpType_PrefixMatch, newPattern, nil
	}

	return planpb.OpType_Match, pattern, nil
}
