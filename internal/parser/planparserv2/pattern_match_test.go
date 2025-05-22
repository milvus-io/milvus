package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func Test_translatePatternMatch(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name        string
		args        args
		wantOp      planpb.OpType
		wantOperand string
		wantErr     bool
	}{
		{
			args:        args{pattern: "prefix%"},
			wantOp:      planpb.OpType_PrefixMatch,
			wantOperand: "prefix",
			wantErr:     false,
		},
		{
			args:        args{pattern: "equal"},
			wantOp:      planpb.OpType_Equal,
			wantOperand: "equal",
			wantErr:     false,
		},
		{
			args:        args{pattern: "%%%%%%"},
			wantOp:      planpb.OpType_PrefixMatch,
			wantOperand: "",
			wantErr:     false,
		},
		{
			args:        args{pattern: "prefix%suffix"},
			wantOp:      planpb.OpType_Match,
			wantOperand: "prefix%suffix",
			wantErr:     false,
		},
		{
			args:        args{pattern: "_0"},
			wantOp:      planpb.OpType_Match,
			wantOperand: "_0",
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOp, gotOperand, err := translatePatternMatch(tt.args.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("translatePatternMatch(%s) error = %v, wantErr %v", tt.args.pattern, err, tt.wantErr)
				return
			}
			if gotOp != tt.wantOp {
				t.Errorf("translatePatternMatch(%s) gotOp = %v, want %v", tt.args.pattern, gotOp, tt.wantOp)
			}
			if gotOperand != tt.wantOperand {
				t.Errorf("translatePatternMatch(%s) gotOperand = %v, want %v", tt.args.pattern, gotOperand, tt.wantOperand)
			}
		})
	}
}

func TestOptimizeLikePattern(t *testing.T) {
	tests := []struct {
		pattern      string
		expectedType planpb.OpType
		expectedStr  string
		expectedOk   bool
	}{
		// inner match
		{"%abc%", planpb.OpType_InnerMatch, "abc", true},
		{"%a\\%b%", planpb.OpType_InnerMatch, "a%b", true},
		{"%a\\_b%", planpb.OpType_InnerMatch, "a_b", true},
		{"%a\\\\%", planpb.OpType_InnerMatch, "a\\\\", true},
		{"%a\t%", planpb.OpType_InnerMatch, "a\t", true},
		{"%", planpb.OpType_PrefixMatch, "", true},
		{"%%", planpb.OpType_PrefixMatch, "", true},
		{"%a%b%", planpb.OpType_Invalid, "", false},
		{"%a_b%", planpb.OpType_Invalid, "", false},
		{"%abc\\", planpb.OpType_PostfixMatch, "abc\\", true},
		{"%核心%", planpb.OpType_InnerMatch, "核心", true},
		{"%核%", planpb.OpType_InnerMatch, "核", true},
		{"%\u6838%", planpb.OpType_InnerMatch, "核", true},
		{"%\u6838%", planpb.OpType_InnerMatch, "\u6838", true},

		// prefix match
		{"abc%", planpb.OpType_PrefixMatch, "abc", true},
		{"a\\%bc%", planpb.OpType_PrefixMatch, "a%bc", true},
		{"a\\_bc%", planpb.OpType_PrefixMatch, "a_bc", true},
		{"_abc%", planpb.OpType_Invalid, "", false},

		// posix match
		{"%abc", planpb.OpType_PostfixMatch, "abc", true},
		{"%a\\_bc", planpb.OpType_PostfixMatch, "a_bc", true},
		{"%abc_", planpb.OpType_Invalid, "", false},
		{"%臥蜜", planpb.OpType_PostfixMatch, "臥蜜", true},
		{"%%臥蜜", planpb.OpType_PostfixMatch, "臥蜜", true},
		{"%\u81e5\u871c", planpb.OpType_PostfixMatch, "臥蜜", true},

		// equal match
		{"abc", planpb.OpType_Equal, "abc", true},
		{"a\\%bc", planpb.OpType_Equal, "a%bc", true},
		{"a\\_bc", planpb.OpType_Equal, "a_bc", true},
		{"abc_", planpb.OpType_Invalid, "", false},

		// null pattern
		{"", planpb.OpType_Equal, "", true},
	}

	for _, test := range tests {
		actualType, actualStr, actualOk := optimizeLikePattern(test.pattern)
		if actualType != test.expectedType || actualStr != test.expectedStr || actualOk != test.expectedOk {
			t.Errorf("optimizeLikePattern(%q) = (%q, %q, %v), expected (%q, %q, %v)",
				test.pattern, actualType, actualStr, actualOk,
				test.expectedType, test.expectedStr, test.expectedOk)
		}
	}
}
