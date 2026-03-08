package ratelimitutil

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestGetQuotaErrorString(t *testing.T) {
	tests := []struct {
		name string
		args commonpb.ErrorCode
		want string
	}{
		{
			name: "Test ErrorCode_ForceDeny",
			args: commonpb.ErrorCode_ForceDeny,
			want: "access has been disabled by the administrator",
		},
		{
			name: "Test ErrorCode_MemoryQuotaExhausted",
			args: commonpb.ErrorCode_MemoryQuotaExhausted,
			want: "memory quota exceeded, please allocate more resources",
		},
		{
			name: "Test ErrorCode_DiskQuotaExhausted",
			args: commonpb.ErrorCode_DiskQuotaExhausted,
			want: "disk quota exceeded, please allocate more resources",
		},
		{
			name: "Test ErrorCode_TimeTickLongDelay",
			args: commonpb.ErrorCode_TimeTickLongDelay,
			want: "time tick long delay",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetQuotaErrorString(tt.args); got != tt.want {
				t.Errorf("GetQuotaErrorString() = %v, want %v", got, tt.want)
			}
		})
	}
}
