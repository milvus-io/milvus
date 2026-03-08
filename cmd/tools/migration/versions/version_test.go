package versions

import (
	"testing"

	"github.com/blang/semver/v4"
)

func TestRange21x(t *testing.T) {
	type args struct {
		version semver.Version
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{version: VersionMax},
			want: false,
		},
		{
			args: args{version: Version230},
			want: false,
		},
		{
			args: args{version: Version220},
			want: false,
		},
		{
			args: args{version: Version210},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Range21x(tt.args.version); got != tt.want {
				t.Errorf("Range21x() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRange22x(t *testing.T) {
	type args struct {
		version semver.Version
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{version: VersionMax},
			want: false,
		},
		{
			args: args{version: Version230},
			want: false,
		},
		{
			args: args{version: Version220},
			want: true,
		},
		{
			args: args{version: Version210},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Range22x(tt.args.version); got != tt.want {
				t.Errorf("Range22x() = %v, want %v", got, tt.want)
			}
		})
	}
}
