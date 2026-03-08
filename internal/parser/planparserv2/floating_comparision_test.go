package planparserv2

import "testing"

func Test_floatingEqual(t *testing.T) {
	type args struct {
		a float64
		b float64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				a: 1.0,
				b: 1.0,
			},
			want: true,
		},
		{
			args: args{
				a: 1.0,
				b: 2.0,
			},
			want: false,
		},
		{
			args: args{
				a: 0.000000001,
				b: 0,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := floatingEqual(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("floatingEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
