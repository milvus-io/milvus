package utils

import (
	"testing"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGracefulStopGrpcServer(t *testing.T) {
	paramtable.Init()

	// expected close by gracefulStop
	s1 := grpc.NewServer()
	GracefulStopGRPCServer(s1)

	// expected not panic
	GracefulStopGRPCServer(nil)
}

func TestIsIPAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		// IPv4 addresses
		{"IPv4 without port", "192.168.1.1", true},
		{"IPv4 with port", "192.168.1.1:8080", true},
		// IPv6 addresses
		{"IPv6 without port", "[2001:db8::1]", true},
		{"IPv6 with port", "[2001:db8::1]:443", true},
		// Non-IP addresses
		{"hostname without port", "example.com", false},
		{"hostname with port", "example.com:443", false},
		{"invalid IP", "999.999.999.999", false},
		// Edge cases
		{"empty string", "", false},
		{"only port", ":8080", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsIPAddress(tt.addr)
			if got != tt.want {
				t.Errorf("IsIPAddress(%q) = %v; want %v", tt.addr, got, tt.want)
			}
		})
	}
}
