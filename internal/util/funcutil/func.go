package funcutil

import (
	"context"
	"net"
	"time"

	"github.com/go-basic/ipv4"
)

func CheckGrpcReady(ctx context.Context, targetCh chan error) {
	select {
	case <-time.After(100 * time.Millisecond):
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

func GetAvailablePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func GetLocalIP() string {
	return ipv4.LocalIP()
}
