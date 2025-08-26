package planparserv2

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

var wildcards = map[byte]struct{}{
	'_': {},
	'%': {},
}

var escapeCharacter byte = '\\'

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

	leading := pattern[0] == '%'
	// trailing percent must not be escaped: number of consecutive '\\' before it must be even
	isUnescapedTrailingPercent := func(s string) bool {
		if s[len(s)-1] != '%' {
			return false
		}
		cnt := 0
		for i := len(s) - 2; i >= 0 && s[i] == '\\'; i-- {
			cnt++
		}
		return cnt%2 == 0
	}
	trailing := isUnescapedTrailingPercent(pattern)

	trimRight := func(s string) string {
		if s == "" || s[len(s)-1] != '%' {
			return s
		}
		for i := len(s) - 2; i >= 0; i-- {
			if s[i] != '%' {
				if isUnescapedTrailingPercent(s[:i+2]) {
					return s[:i+1]
				}
				return s[:i+2]
			}
		}
		return ""
	}

	switch {
	case leading && trailing:
		inner := pattern[1 : len(pattern)-1]
		trimmed := strings.TrimLeft(inner, "%")
		trimmed = trimRight(trimmed)
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
		trimmed := trimRight(pattern[:len(pattern)-1])
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

func escapeStringWithWildcards(str string) (op planpb.OpType, operand string, err error) {
	// 1. Check and remove quotes
	if len(str) < 2 {
		return planpb.OpType_Invalid, "", fmt.Errorf("invalid string: too short")
	}

	if str[0] == '"' && str[len(str)-1] == '"' {
		str = str[1 : len(str)-1]
	} else if str[0] == '\'' && str[len(str)-1] == '\'' {
		str = str[1 : len(str)-1]
	} else {
		return planpb.OpType_Invalid, "", fmt.Errorf("invalid string: missing or mismatched quotes")
	}

	// Handle empty string case
	if len(str) == 0 {
		return planpb.OpType_Equal, "", nil
	}

	// 2. Process escape characters and build intermediate representation
	var processed strings.Builder // Processed string with escape chars resolved

	for i := 0; i < len(str); i++ {
		c := str[i]
		if c == '\\' && i+1 < len(str) {
			next := str[i+1]
			switch next {
			case 'n':
				processed.WriteByte('\n')
				i++
			case 't':
				processed.WriteByte('\t')
				i++
			case 'r':
				processed.WriteByte('\r')
				i++
			case '\\':
				processed.WriteByte('\\')
				i++
			case '"':
				processed.WriteByte('"')
				i++
			case '\'':
				processed.WriteByte('\'')
				i++
			case '_', '%':
				// Escaped wildcards: mark them as literal by using a special prefix
				processed.WriteByte('\x00') // Use null byte as escape marker
				processed.WriteByte(next)
				i++
			default:
				// Other escape characters, keep backslash
				processed.WriteByte(c)
			}
		} else {
			processed.WriteByte(c)
		}
	}

	pattern := processed.String()

	// 3. Analyze pattern and build outputs based on optimizeLikePattern logic
	if len(pattern) == 0 {
		return planpb.OpType_Equal, "", nil
	}

	// Check for pure wildcard patterns
	if pattern == "%" || pattern == "%%" {
		return planpb.OpType_PrefixMatch, "", nil
	}

	// Analyze pattern structure
	leading := len(pattern) > 0 && pattern[0] == '%'
	trailing := len(pattern) > 0 && pattern[len(pattern)-1] == '%'

	// Count consecutive trailing percents (unescaped)
	trailingPercentEnd := len(pattern)
	if trailing {
		for i := len(pattern) - 1; i >= 0 && pattern[i] == '%'; i-- {
			trailingPercentEnd = i
		}
	}

	// Extract and process middle part
	start := 0
	if leading {
		// Skip leading percents
		for start < len(pattern) && pattern[start] == '%' {
			start++
		}
	}

	end := trailingPercentEnd
	middle := pattern[start:end]

	// Process middle part: check for internal wildcards and build operand/regex
	var cleanOperand strings.Builder
	var regexBuf strings.Builder
	hasInternalWildcards := false

	for i := 0; i < len(middle); i++ {
		c := middle[i]
		if c == '\x00' && i+1 < len(middle) {
			// Escaped wildcard - treat as literal
			next := middle[i+1]
			cleanOperand.WriteByte(next)
			regexBuf.WriteByte(next)
			i++
		} else if c == '_' {
			// Unescaped underscore wildcard
			regexBuf.WriteString("[\\s\\S]")
			hasInternalWildcards = true
		} else if c == '%' {
			// Internal percent wildcard
			regexBuf.WriteString("[\\s\\S]*")
			hasInternalWildcards = true
		} else {
			cleanOperand.WriteByte(c)
			regexBuf.WriteByte(c)
		}
	}

	// 4. Determine operation type
	operandStr := cleanOperand.String()

	// If has internal wildcards, return regex
	if hasInternalWildcards {
		var fullRegex strings.Builder
		if leading {
			fullRegex.WriteString("[\\s\\S]*")
		}
		fullRegex.WriteString(regexBuf.String())
		if trailing {
			fullRegex.WriteString("[\\s\\S]*")
		}
		return planpb.OpType_Match, fullRegex.String(), nil
	}

	// No internal wildcards - can optimize
	switch {
	case leading && trailing:
		if len(operandStr) == 0 {
			return planpb.OpType_PrefixMatch, "", nil
		}
		return planpb.OpType_InnerMatch, operandStr, nil
	case leading:
		return planpb.OpType_PostfixMatch, operandStr, nil
	case trailing:
		return planpb.OpType_PrefixMatch, operandStr, nil
	default:
		return planpb.OpType_Equal, operandStr, nil
	}
}
