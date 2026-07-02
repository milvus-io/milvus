package metastore

import "testing"

func TestAlterType_String(t *testing.T) {
	tests := []struct {
		name string
		t    AlterType
		want string
	}{
		{
			t:    ADD,
			want: "ADD",
		},
		{
			t:    DELETE,
			want: "DELETE",
		},
		{
			t:    MODIFY,
			want: "MODIFY",
		},
		{
			t:    -1,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
