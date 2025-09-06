package planparserv2

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func escapeStringWithWildcards(str string) (op planpb.OpType, operand string, err error) {
	// 1. Check and remove quotes, handle encoding prefixes
	if len(str) < 2 {
		return planpb.OpType_Invalid, "", fmt.Errorf("invalid string: too short")
	}

	// Handle encoding prefixes: u8, u, U, L
	originalStr := str
	if len(str) >= 3 {
		if str[:2] == "u8" || str[:1] == "u" || str[:1] == "U" || str[:1] == "L" {
			if str[:2] == "u8" {
				str = str[2:]
			} else {
				str = str[1:]
			}
		}
	}

	if len(str) < 2 {
		return planpb.OpType_Invalid, "", fmt.Errorf("invalid string: too short after prefix")
	}

	if str[0] == '"' && str[len(str)-1] == '"' {
		str = str[1 : len(str)-1]
	} else if str[0] == '\'' && str[len(str)-1] == '\'' {
		str = str[1 : len(str)-1]
	} else {
		return planpb.OpType_Invalid, "", fmt.Errorf("invalid string: missing or mismatched quotes in %s", originalStr)
	}

	// Handle empty string case
	if len(str) == 0 {
		return planpb.OpType_Equal, "", nil
	}

	// Handle pure wildcard patterns (one or more consecutive %)
	allPercent := true
	for i := 0; i < len(str); i++ {
		if str[i] != '%' {
			allPercent = false
			break
		}
	}
	if allPercent && len(str) > 0 {
		return planpb.OpType_PrefixMatch, "", nil
	}

	// Single pass processing: handle escape sequences and determine pattern type
	var resultBuf strings.Builder  // Final result string with wildcards converted to regex
	var literalBuf strings.Builder // Clean literal string for optimization cases
	hasLeadingPercent := false
	hasTrailingPercent := false
	hasMiddleWildcards := false

	// Check for leading percent and skip consecutive ones
	if len(str) > 0 && str[0] == '%' {
		hasLeadingPercent = true
	}

	// Check for unescaped trailing percent - scan backwards to handle consecutive %
	if len(str) > 0 && str[len(str)-1] == '%' {
		// Count consecutive backslashes before the last %
		escapeCount := 0
		for i := len(str) - 2; i >= 0 && str[i] == '\\'; i-- {
			escapeCount++
		}
		// If even number of backslashes (including 0), the % is not escaped
		if escapeCount%2 == 0 {
			hasTrailingPercent = true
		}
	}

	// Determine the range to process (exclude leading/trailing %)
	start := 0
	if hasLeadingPercent {
		// Skip all consecutive leading % characters
		for start < len(str) && str[start] == '%' {
			start++
		}
	}

	end := len(str)
	if hasTrailingPercent {
		// Skip all consecutive trailing % characters
		for end > start && str[end-1] == '%' {
			end--
		}
		escapeCnt := 0
		for i := end - 1; i >= start && str[i] == '\\'; i-- {
			escapeCnt++
		}
		if escapeCnt%2 == 1 {
			end++
		}
	}

	// Process the middle part
	for i := start; i < end; i++ {
		c := str[i]
		switch c {
		case '\\':
			if i+1 >= end {
				// Backslash at the end of string - this is an error
				return planpb.OpType_Invalid, "", fmt.Errorf("invalid escape sequence: backslash at end of string")
			}
			next := str[i+1]
			switch next {
			case 'a':
				resultBuf.WriteByte('\a') // Bell/alert
				literalBuf.WriteByte('\a')
				i++
			case 'b':
				resultBuf.WriteByte('\b') // Backspace
				literalBuf.WriteByte('\b')
				i++
			case 'f':
				resultBuf.WriteByte('\f') // Form feed
				literalBuf.WriteByte('\f')
				i++
			case 'n':
				resultBuf.WriteByte('\n')
				literalBuf.WriteByte('\n')
				i++
			case 'r':
				resultBuf.WriteByte('\r')
				literalBuf.WriteByte('\r')
				i++
			case 't':
				resultBuf.WriteByte('\t')
				literalBuf.WriteByte('\t')
				i++
			case 'v':
				resultBuf.WriteByte('\v') // Vertical tab
				literalBuf.WriteByte('\v')
				i++
			case '\\':
				resultBuf.WriteByte('\\')
				literalBuf.WriteByte('\\')
				i++
			case '"':
				resultBuf.WriteByte('"')
				literalBuf.WriteByte('"')
				i++
			case '\'':
				resultBuf.WriteByte('\'')
				literalBuf.WriteByte('\'')
				i++
			case '?':
				resultBuf.WriteByte('?')
				literalBuf.WriteByte('?')
				i++
			case '_', '%':
				// Escaped wildcards - treat as literal characters
				resultBuf.WriteByte(next)
				literalBuf.WriteByte(next)
				i++
			case 'x':
				// Hexadecimal escape sequence \xHH
				if i+4 > end {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid hexadecimal escape sequence: incomplete")
				}
				hexStr := str[i+2 : i+4]
				codePoint, err := strconv.ParseUint(hexStr, 16, 8)
				if err != nil {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid hexadecimal escape sequence: %s", hexStr)
				}
				resultBuf.WriteByte(byte(codePoint))
				literalBuf.WriteByte(byte(codePoint))
				i += 3
			case 'u':
				// Unicode escape sequence \uXXXX
				if i+6 > end {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid Unicode escape sequence: incomplete")
				}
				hexStr := str[i+2 : i+6]
				codePoint, err := strconv.ParseUint(hexStr, 16, 16)
				if err != nil {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid Unicode escape sequence: %s", hexStr)
				}
				r := rune(codePoint)
				resultBuf.WriteRune(r)
				literalBuf.WriteRune(r)
				i += 5
			case 'U':
				// Extended Unicode escape sequence \UXXXXXXXX
				if i+10 > end {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid extended Unicode escape sequence: incomplete")
				}
				hexStr := str[i+2 : i+10]
				codePoint, err := strconv.ParseUint(hexStr, 16, 32)
				if err != nil {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid extended Unicode escape sequence: %s", hexStr)
				}
				r := rune(codePoint)
				resultBuf.WriteRune(r)
				literalBuf.WriteRune(r)
				i += 9
			case '0', '1', '2', '3', '4', '5', '6', '7':
				// Octal escape sequence \ooo (1-3 octal digits)
				octalStr := string(next)
				consumed := 1
				// Check for additional octal digits (up to 3 total)
				for consumed < 3 && i+1+consumed < end {
					nextChar := str[i+1+consumed]
					if nextChar >= '0' && nextChar <= '7' {
						octalStr += string(nextChar)
						consumed++
					} else {
						break
					}
				}
				codePoint, err := strconv.ParseUint(octalStr, 8, 8)
				if err != nil {
					return planpb.OpType_Invalid, "", fmt.Errorf("invalid octal escape sequence: %s", octalStr)
				}
				resultBuf.WriteByte(byte(codePoint))
				literalBuf.WriteByte(byte(codePoint))
				i += consumed
			default:
				// Keep backslash for unknown escape sequences
				resultBuf.WriteByte(c)
				literalBuf.WriteByte(c)
			}
		case '_':
			// Unescaped underscore wildcard
			resultBuf.WriteString("[\\s\\S]")
			hasMiddleWildcards = true
		case '%':
			// Unescaped percent wildcard - collapse consecutive ones
			resultBuf.WriteString("[\\s\\S]*")
			hasMiddleWildcards = true
			// Skip additional consecutive % characters for optimization
			for i+1 < end && str[i+1] == '%' {
				i++
			}
		default:
			// Regular character
			resultBuf.WriteByte(c)
			literalBuf.WriteByte(c)
		}
	}

	// Determine operation type and build final result
	literalStr := literalBuf.String()

	if hasMiddleWildcards {
		// Has internal wildcards - must use regex match
		var finalResult strings.Builder
		if hasLeadingPercent {
			finalResult.WriteString("[\\s\\S]*")
		}
		finalResult.WriteString(resultBuf.String())
		if hasTrailingPercent {
			finalResult.WriteString("[\\s\\S]*")
		}
		return planpb.OpType_Match, finalResult.String(), nil
	}

	// No internal wildcards - can optimize
	switch {
	case hasLeadingPercent && hasTrailingPercent:
		if len(literalStr) == 0 {
			// Pure % pattern
			return planpb.OpType_PrefixMatch, "", nil
		}
		return planpb.OpType_InnerMatch, literalStr, nil
	case hasLeadingPercent:
		return planpb.OpType_PostfixMatch, literalStr, nil
	case hasTrailingPercent:
		return planpb.OpType_PrefixMatch, literalStr, nil
	default:
		return planpb.OpType_Equal, literalStr, nil
	}
}
