package planparserv2

import (
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

var wildcards = map[byte]struct{}{
	'_': {},
	'%': {},
}

var escapeCharacter byte = '\\'

// trailingWildcard reports whether the pattern ends in an unescaped '%'.
// A trailing '%' preceded by an odd number of backslashes is escaped (a
// literal '%'), so it is not a wildcard.
func trailingWildcard(pattern string) bool {
	if len(pattern) == 0 || pattern[len(pattern)-1] != '%' {
		return false
	}
	backslashes := 0
	for i := len(pattern) - 2; i >= 0 && pattern[i] == escapeCharacter; i-- {
		backslashes++
	}
	return backslashes%2 == 0
}

func optimizeLikePattern(pattern string) (planpb.OpType, string, bool) {
	if len(pattern) == 0 {
		return planpb.OpType_Equal, "", true
	}

	if pattern == "%" || pattern == "%%" {
		return planpb.OpType_PrefixMatch, "", true
	}

	process := func(s string) (string, bool) {
		var buf strings.Builder
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c == escapeCharacter && i+1 < len(s) {
				next := s[i+1]
				if _, ok := wildcards[next]; ok {
					buf.WriteByte(next)
					i++
					continue
				}
			}
			if _, ok := wildcards[c]; ok {
				return "", false
			}
			buf.WriteByte(c)
		}
		return buf.String(), true
	}

	// A leading '%' is always a wildcard (nothing can escape it), but a trailing
	// '%' preceded by an odd number of backslashes is escaped — a literal '%',
	// not a wildcard — so it must not trigger prefix/inner optimization.
	leading := pattern[0] == '%'
	trailing := trailingWildcard(pattern)

	switch {
	case leading && trailing:
		inner := pattern[1 : len(pattern)-1]
		trimmed := strings.TrimLeft(inner, "%")
		trimmed = strings.TrimRight(trimmed, "%")
		if subStr, valid := process(trimmed); valid {
			// if subStr is empty, it means the pattern is all %,
			// return prefix match and empty operand, means all match
			if len(subStr) == 0 {
				return planpb.OpType_PrefixMatch, "", true
			}
			return planpb.OpType_InnerMatch, subStr, true
		}
	case leading:
		trimmed := strings.TrimLeft(pattern[1:], "%")
		if subStr, valid := process(trimmed); valid {
			return planpb.OpType_PostfixMatch, subStr, true
		}
	case trailing:
		trimmed := strings.TrimRight(pattern[:len(pattern)-1], "%")
		if subStr, valid := process(trimmed); valid {
			return planpb.OpType_PrefixMatch, subStr, true
		}
	default:
		if subStr, valid := process(pattern); valid {
			return planpb.OpType_Equal, subStr, true
		}
	}
	return planpb.OpType_Invalid, "", false
}

// translatePatternMatch translates pattern to related op type and operand.
func translatePatternMatch(pattern string) (op planpb.OpType, operand string, err error) {
	op, operand, ok := optimizeLikePattern(pattern)
	if ok {
		return op, operand, nil
	}

	return planpb.OpType_Match, pattern, nil
}
