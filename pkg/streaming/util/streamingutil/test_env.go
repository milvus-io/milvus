//go:build test
// +build test

package streamingutil

import "os"

// UnsetStreamingServiceEnabled unsets the env that indicates whether the streaming service is enabled.
func UnsetStreamingServiceEnabled() {
	err := os.Setenv(MilvusStreamingServiceEnabled, "0")
	if err != nil {
		panic(err)
	}
}
