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

// likeToken is one logical byte of a LIKE pattern after escape processing.
// wildcard is true only for an UNescaped '%' or '_'; every other byte
// (including an escaped wildcard or an escaped backslash) is a literal.
type likeToken struct {
	b        byte
	wildcard bool
}

// scanLikePattern walks the pattern once and applies the canonical escape
// model used by the C++ matcher (translate_pattern_match_to_regex /
// extract_fixed_prefix_from_pattern in internal/core/src/common/RegexQuery.cpp):
//
//   - a backslash escapes the NEXT byte, whatever it is, so the backslash is
//     consumed and the following byte becomes a literal ('\\' -> '\', '\%' ->
//     '%', '\x' -> 'x');
//   - an unescaped '%' or '_' is a wildcard;
//   - a lone trailing backslash (nothing to escape) is invalid.
//
// It returns ok=false for the trailing-backslash case so the caller falls back
// to OpType_Match, where the C++ side raises the matching ExprInvalid error.
// Keeping this in lock-step with the C++ model is what guarantees the optimized
// Equal/Prefix/Postfix/Inner paths return the same rows as the regex path.
func scanLikePattern(pattern string) ([]likeToken, bool) {
	tokens := make([]likeToken, 0, len(pattern))
	escapeMode := false
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if escapeMode {
			tokens = append(tokens, likeToken{b: c, wildcard: false})
			escapeMode = false
			continue
		}
		if c == escapeCharacter {
			escapeMode = true
			continue
		}
		if _, ok := wildcards[c]; ok {
			tokens = append(tokens, likeToken{b: c, wildcard: true})
			continue
		}
		tokens = append(tokens, likeToken{b: c, wildcard: false})
	}
	if escapeMode {
		// trailing backslash with nothing to escape
		return nil, false
	}
	return tokens, true
}

// literal concatenates the literal bytes of the given tokens. The caller must
// ensure none of them is a wildcard (anyWildcard == false).
func literal(tokens []likeToken) string {
	var buf strings.Builder
	buf.Grow(len(tokens))
	for _, t := range tokens {
		buf.WriteByte(t.b)
	}
	return buf.String()
}

func anyWildcard(tokens []likeToken) bool {
	for _, t := range tokens {
		if t.wildcard {
			return true
		}
	}
	return false
}

// optimizeLikePattern lowers a LIKE pattern into a cheaper operator when the
// only wildcards are leading and/or trailing '%'. It returns ok=false when the
// pattern cannot be optimized (an unescaped '_' or an interior '%', or a
// dangling escape) so the caller keeps the full OpType_Match path.
func optimizeLikePattern(pattern string) (planpb.OpType, string, bool) {
	tokens, ok := scanLikePattern(pattern)
	if !ok {
		return planpb.OpType_Invalid, "", false
	}
	if len(tokens) == 0 {
		return planpb.OpType_Equal, "", true
	}

	// Count the leading and trailing runs of '%' wildcards. A single-char '_'
	// wildcard can never become a prefix/postfix boundary, so it is left for the
	// generic Match path via the anyWildcard check on the core below.
	leadingPercent := 0
	for leadingPercent < len(tokens) && tokens[leadingPercent].wildcard && tokens[leadingPercent].b == '%' {
		leadingPercent++
	}
	// The whole pattern is '%'s -> match everything.
	if leadingPercent == len(tokens) {
		return planpb.OpType_PrefixMatch, "", true
	}
	trailingPercent := 0
	for trailingPercent < len(tokens)-leadingPercent &&
		tokens[len(tokens)-1-trailingPercent].wildcard &&
		tokens[len(tokens)-1-trailingPercent].b == '%' {
		trailingPercent++
	}

	core := tokens[leadingPercent : len(tokens)-trailingPercent]
	// Any wildcard left in the core (an interior '%' or any '_') means the
	// pattern is not a plain prefix/postfix/inner match.
	if anyWildcard(core) {
		return planpb.OpType_Invalid, "", false
	}
	operand := literal(core)

	switch {
	case leadingPercent > 0 && trailingPercent > 0:
		return planpb.OpType_InnerMatch, operand, true
	case leadingPercent > 0:
		return planpb.OpType_PostfixMatch, operand, true
	case trailingPercent > 0:
		return planpb.OpType_PrefixMatch, operand, true
	default:
		return planpb.OpType_Equal, operand, true
	}
}

// translatePatternMatch translates pattern to related op type and operand.
func translatePatternMatch(pattern string) (op planpb.OpType, operand string, err error) {
	op, operand, ok := optimizeLikePattern(pattern)
	if ok {
		return op, operand, nil
	}

	return planpb.OpType_Match, pattern, nil
}
