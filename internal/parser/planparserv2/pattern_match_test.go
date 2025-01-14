package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/proto/planpb"
)

func Test_hasWildcards(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				pattern: "no-wildcards",
			},
			want: false,
		},
		{
			args: args{
				pattern: "has\\%",
			},
			want: false,
		},
		{
			args: args{
				pattern: "%",
			},
			want: true,
		},
		{
			args: args{
				pattern: "has%",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasWildcards(tt.args.pattern); got != tt.want {
				t.Errorf("hasWildcards(%s) = %v, want %v", tt.args.pattern, got, tt.want)
			}
		})
	}
}

func Test_findLocOfLastWildcard(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			args: args{
				pattern: "no-wildcards",
			},
			want: 11,
		},
		{
			args: args{
				pattern: "only\\%",
			},
			want: 5,
		},
		{
			args: args{
				pattern: "prefix%%",
			},
			want: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := findLastNotOfWildcards(tt.args.pattern); got != tt.want {
				t.Errorf("findLastNotOfWildcards(%s) = %v, want %v", tt.args.pattern, got, tt.want)
			}
		})
	}
}

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
