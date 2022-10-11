package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitBySeparator(t *testing.T) {
	tsKey := "435783141193354561_ts435783141193154564"
	k, ts, err := SplitBySeparator(tsKey)
	assert.NoError(t, err)
	assert.Equal(t, "435783141193354561", k)
	assert.Equal(t, Timestamp(435783141193154564), ts)
}

func TestGetFileName(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{p: "snapshots/root-coord/collection/436611447439428929_ts436611447439228933"},
			want: "436611447439428929_ts436611447439228933",
		},
		{
			args: args{p: "root-coord/collection/436611447439428929"},
			want: "436611447439428929",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetFileName(tt.args.p), "GetFileName(%v)", tt.args.p)
		})
	}
}
