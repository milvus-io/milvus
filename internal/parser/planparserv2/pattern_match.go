package planparserv2

import (
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

var wildcards = map[byte]struct{}{
	'_': {},
	'%': {},
}

var escapeCharacter byte = '\\'

// hasWildcards returns true if pattern contains any wildcard.
func hasWildcards(pattern string) bool {
	l := len(pattern)
	i := l - 1
	for ; i >= 0; i-- {
		_, ok := wildcards[pattern[i]]
		if ok {
			if i > 0 && pattern[i-1] == escapeCharacter {
				i--
				continue
			}
			return true
		}
	}
	return false
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
func translatePatternMatch(pattern string, hasNotOp bool) (op planpb.OpType, operand string, err error) {
	// TODO: support OpType_PostfixMatch
	l := len(pattern)
	loc := findLastNotOfWildcards(pattern)

	if loc < 0 {
		// always match.
		if hasNotOp {
			return planpb.OpType_PrefixNotMatch, "", nil
		}
		return planpb.OpType_PrefixMatch, "", nil
	}

	exist := hasWildcards(pattern[:loc+1])
	if loc >= l-1 && !exist {
		// equal match.
		if hasNotOp {
			return planpb.OpType_NotEqual, pattern, nil
		}
		return planpb.OpType_Equal, pattern, nil
	}
	if !exist {
		// prefix match.
		if hasNotOp {
			return planpb.OpType_PrefixNotMatch, pattern[:loc+1], nil
		}
		return planpb.OpType_PrefixMatch, pattern[:loc+1], nil
	}

	if hasNotOp {
		return planpb.OpType_NotMatch, pattern, nil
	}
	return planpb.OpType_Match, pattern, nil
}
