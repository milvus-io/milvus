package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func Test_escapeStringWithWildcards(t *testing.T) {
	tests := []struct {
		pattern       string
		expectedType  planpb.OpType
		expectedStr   string
		expectedValid bool
	}{
		// inner match
		{`"%abc%"`, planpb.OpType_InnerMatch, "abc", true},
		{`"%a\%b%"`, planpb.OpType_InnerMatch, "a%b", true},
		{`"%a\_b%"`, planpb.OpType_InnerMatch, "a_b", true},
		{`"%a\\%"`, planpb.OpType_InnerMatch, "a\\", true},
		{`"%a\t%"`, planpb.OpType_InnerMatch, "a\t", true},
		{`"%"`, planpb.OpType_PrefixMatch, "", true},
		{`"%%"`, planpb.OpType_PrefixMatch, "", true},
		{`"a\%%"`, planpb.OpType_PrefixMatch, "a%", true},
		{`"a\\\\%%"`, planpb.OpType_PrefixMatch, "a\\\\", true},
		{`"%a%b%"`, planpb.OpType_Match, "[\\s\\S]*a[\\s\\S]*b[\\s\\S]*", true},
		{`"%a_b%"`, planpb.OpType_Match, "[\\s\\S]*a[\\s\\S]b[\\s\\S]*", true},
		{`"%abc\\"`, planpb.OpType_PostfixMatch, "abc\\", true},
		{`"%核心%"`, planpb.OpType_InnerMatch, "核心", true},
		{`"%核%"`, planpb.OpType_InnerMatch, "核", true},
		{`"%\u6838%"`, planpb.OpType_InnerMatch, "核", true},
		{`"%\u6838%"`, planpb.OpType_InnerMatch, "\u6838", true},

		// prefix match
		{`"abc%"`, planpb.OpType_PrefixMatch, "abc", true},
		{`"a\%bc%"`, planpb.OpType_PrefixMatch, "a%bc", true},
		{`"a\_bc%"`, planpb.OpType_PrefixMatch, "a_bc", true},
		{`"_abc%"`, planpb.OpType_Match, "[\\s\\S]abc[\\s\\S]*", true},

		// posix match
		{`"%abc"`, planpb.OpType_PostfixMatch, "abc", true},
		{`"%a\_bc"`, planpb.OpType_PostfixMatch, "a_bc", true},
		{`"%abc_"`, planpb.OpType_Match, "[\\s\\S]*abc[\\s\\S]", true},
		{`"%臥蜜"`, planpb.OpType_PostfixMatch, "臥蜜", true},
		{`"%%臥蜜"`, planpb.OpType_PostfixMatch, "臥蜜", true},
		{`"%\u81e5\u871c"`, planpb.OpType_PostfixMatch, "臥蜜", true},

		// equal match
		{`"abc"`, planpb.OpType_Equal, "abc", true},
		{`"a\%bc"`, planpb.OpType_Equal, "a%bc", true},
		{`"a\_bc"`, planpb.OpType_Equal, "a_bc", true},
		{`"abc_"`, planpb.OpType_Match, "abc[\\s\\S]", true},

		// null pattern
		{`""`, planpb.OpType_Equal, "", true},

		// encoding prefix tests
		{`u8"hello"`, planpb.OpType_Equal, "hello", true},
		{`u"world"`, planpb.OpType_Equal, "world", true},
		{`U"test"`, planpb.OpType_Equal, "test", true},
		{`L"example"`, planpb.OpType_Equal, "example", true},
		{`u8"hello%"`, planpb.OpType_PrefixMatch, "hello", true},
		{`u"%world"`, planpb.OpType_PostfixMatch, "world", true},
		{`U"%test%"`, planpb.OpType_InnerMatch, "test", true},
		{`L"ex_ample"`, planpb.OpType_Match, "ex[\\s\\S]ample", true},

		// basic escape sequences
		{`"bell\a"`, planpb.OpType_Equal, "bell\a", true},
		{`"back\b"`, planpb.OpType_Equal, "back\b", true},
		{`"form\f"`, planpb.OpType_Equal, "form\f", true},
		{`"vert\v"`, planpb.OpType_Equal, "vert\v", true},
		{`"quest\?"`, planpb.OpType_Equal, "quest?", true},
		{`"tab\there"`, planpb.OpType_Equal, "tab\there", true},
		{`"new\nline"`, planpb.OpType_Equal, "new\nline", true},
		{`"car\rreturn"`, planpb.OpType_Equal, "car\rreturn", true},
		{`"back\\slash"`, planpb.OpType_Equal, "back\\slash", true},

		// octal escape sequences
		{`"\101"`, planpb.OpType_Equal, "A", true},           // \101 = A (65)
		{`"\12"`, planpb.OpType_Equal, "\n", true},           // \12 = newline (10)
		{`"\377"`, planpb.OpType_Equal, "\xff", true},        // \377 = 255
		{`"\7"`, planpb.OpType_Equal, "\a", true},            // \7 = bell (7)
		{`"abc\141bc"`, planpb.OpType_Equal, "abcabc", true}, // \141 = 'a'
		{`"test\101%"`, planpb.OpType_PrefixMatch, "testA", true},

		// hexadecimal escape sequences
		{`"\x41"`, planpb.OpType_Equal, "A", true},    // \x41 = A (65)
		{`"\x0A"`, planpb.OpType_Equal, "\n", true},   // \x0A = newline (10)
		{`"\xff"`, planpb.OpType_Equal, "\xff", true}, // \xff = 255
		{`"\x48\x65\x6C\x6C\x6F"`, planpb.OpType_Equal, "Hello", true},
		{`"test\x41%"`, planpb.OpType_PrefixMatch, "testA", true},

		// unicode escape sequences (4-digit)
		{`"\u0041"`, planpb.OpType_Equal, "A", true},
		{`"\u4e16\u754c"`, planpb.OpType_Equal, "世界", true},
		{`"Hello\u4e16\u754c"`, planpb.OpType_Equal, "Hello世界", true},
		{`"\u6838%"`, planpb.OpType_PrefixMatch, "核", true},

		// unicode escape sequences (8-digit)
		{`"\U00000041"`, planpb.OpType_Equal, "A", true},
		{`"\U00004e16\U0000754c"`, planpb.OpType_Equal, "世界", true},
		{`"Hello\U00004e16\U0000754c"`, planpb.OpType_Equal, "Hello世界", true},
		{`"\U00006838%"`, planpb.OpType_PrefixMatch, "核", true},

		// mixed escape sequences
		{`u8"Hello\u4e16\u754c\x21"`, planpb.OpType_Equal, "Hello世界!", true},
		{`L"\141\142\143"`, planpb.OpType_Equal, "abc", true},
		{`u"\x48\x65\x6C\x6C\x6F"`, planpb.OpType_Equal, "Hello", true},
		{`"\u6838_\x41"`, planpb.OpType_Match, "核[\\s\\S]A", true},

		// invalid patterns
		{`"abc\"`, planpb.OpType_Invalid, "", false},
		{`"incomplete\x4"`, planpb.OpType_Invalid, "", false},       // incomplete hex
		{`"incomplete\u123"`, planpb.OpType_Invalid, "", false},     // incomplete unicode
		{`"incomplete\U1234567"`, planpb.OpType_Invalid, "", false}, // incomplete extended unicode
		{`"invalid\xGH"`, planpb.OpType_Invalid, "", false},         // invalid hex chars
		{`"trailing\"`, planpb.OpType_Invalid, "", false},           // trailing backslash
		{`u8`, planpb.OpType_Invalid, "", false},                    // too short with prefix
		{`u"`, planpb.OpType_Invalid, "", false},                    // too short with prefix
	}

	for _, test := range tests {
		actualType, actualStr, actualErr := escapeStringWithWildcards(test.pattern)
		if actualType != test.expectedType || actualStr != test.expectedStr || (actualErr == nil) != test.expectedValid {
			t.Errorf("escapeStringWithWildcards(%q) = (%q, %q, %v), expected (%q, %q, %v)",
				test.pattern, actualType, actualStr, actualErr,
				test.expectedType, test.expectedStr, test.expectedValid)
		}
	}
}
