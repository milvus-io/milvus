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
		if pattern[i] == escapeCharacter && i+1 < len(pattern) {
			next := pattern[i+1]
			if _, ok := wildcards[next]; ok {
				result.WriteByte(next)
				i++
				continue
			}
		}

		if _, ok := wildcards[pattern[i]]; ok {
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

func optimizeLikePattern(pattern string) (planpb.OpType, string, bool) {
	if len(pattern) == 0 {
		return planpb.OpType_Equal, "", false
	}

	if pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		if len(pattern) < 3 {
			return planpb.OpType_Invalid, "", false
		}
		var subStrBuilder strings.Builder
		escaping := false
		for i := 1; i < len(pattern)-1; i++ {
			c := pattern[i]
			if escaping {
				subStrBuilder.WriteByte(c)
				escaping = false
			} else if c == '\\' {
				escaping = true
			} else if c == '%' || c == '_' {
				return planpb.OpType_Invalid, "", false
			} else {
				subStrBuilder.WriteByte(c)
			}
		}
		if escaping {
			return planpb.OpType_Invalid, "", false
		}
		return planpb.OpType_InnerMatch, subStrBuilder.String(), true
	} else if pattern[0] == '%' {
		var subStrBuilder strings.Builder
		escaping := false
		for i := 1; i < len(pattern); i++ {
			c := pattern[i]
			if escaping {
				subStrBuilder.WriteByte(c)
				escaping = false
			} else if c == '\\' {
				escaping = true
			} else if c == '_' || c == '%' {
				return planpb.OpType_Invalid, "", false
			} else {
				subStrBuilder.WriteByte(c)
			}
		}
		if escaping {
			return planpb.OpType_Invalid, "", false
		}
		return planpb.OpType_PostfixMatch, subStrBuilder.String(), true
	} else if pattern[len(pattern)-1] == '%' {
		var subStrBuilder strings.Builder
		escaping := false
		for i := 0; i < len(pattern)-1; i++ {
			c := pattern[i]
			if escaping {
				subStrBuilder.WriteByte(c)
				escaping = false
			} else if c == '\\' {
				escaping = true
			} else if c == '_' || c == '%' {
				return planpb.OpType_Invalid, "", false
			} else {
				subStrBuilder.WriteByte(c)
			}
		}
		if escaping {
			return planpb.OpType_Invalid, "", false
		}
		return planpb.OpType_PrefixMatch, subStrBuilder.String(), true
	} else {
		var subStrBuilder strings.Builder
		escaping := false
		for i := 0; i < len(pattern); i++ {
			c := pattern[i]
			if escaping {
				subStrBuilder.WriteByte(c)
				escaping = false
			} else if c == '\\' {
				escaping = true
			} else if c == '%' || c == '_' {
				return planpb.OpType_Invalid, "", false
			} else {
				subStrBuilder.WriteByte(c)
			}
		}
		if escaping {
			return planpb.OpType_Invalid, "", false
		}

		return planpb.OpType_Equal, subStrBuilder.String(), true
	}
}

func isAllPercentLoop(s string) bool {
	for _, char := range s {
		if char != '%' {
			return false
		}
	}
	return true
}

// translatePatternMatch translates pattern to related op type and operand.
func translatePatternMatch(pattern string) (op planpb.OpType, operand string, err error) {
	op, operand, ok := optimizeLikePattern(pattern)
	if ok {
		return op, operand, nil
	}

	// if pattern is all %, return prefix match and empty operand, means all match
	if isAllPercentLoop(pattern) {
		return planpb.OpType_PrefixMatch, "", nil
	}

	return planpb.OpType_Match, pattern, nil
}
