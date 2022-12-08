package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSystemField(t *testing.T) {
	type args struct {
		fieldID int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{fieldID: StartOfUserFieldID},
			want: false,
		},
		{
			args: args{fieldID: StartOfUserFieldID + 1},
			want: false,
		},
		{
			args: args{fieldID: TimeStampField},
			want: true,
		},
		{
			args: args{fieldID: RowIDField},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsSystemField(tt.args.fieldID), "IsSystemField(%v)", tt.args.fieldID)
		})
	}
}
