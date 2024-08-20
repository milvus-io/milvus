package streamingutil

import "os"

const MilvusStreamingServiceEnabled = "MILVUS_STREAMING_SERVICE_ENABLED"

// IsStreamingServiceEnabled returns whether the streaming service is enabled.
func IsStreamingServiceEnabled() bool {
	// TODO: check if the environment variable MILVUS_STREAMING_SERVICE_ENABLED is set
	return os.Getenv(MilvusStreamingServiceEnabled) == "1"
}

// SetStreamingServiceEnabled sets the env that indicates whether the streaming service is enabled.
func SetStreamingServiceEnabled() {
	err := os.Setenv(MilvusStreamingServiceEnabled, "1")
	if err != nil {
		panic(err)
	}
}

// UnsetStreamingServiceEnabled unsets the env that indicates whether the streaming service is enabled.
func UnsetStreamingServiceEnabled() {
	err := os.Setenv(MilvusStreamingServiceEnabled, "0")
	if err != nil {
		panic(err)
	}
}

// MustEnableStreamingService panics if the streaming service is not enabled.
func MustEnableStreamingService() {
	if !IsStreamingServiceEnabled() {
		panic("start a streaming node without enabling streaming service, please set environment variable MILVUS_STREAMING_SERVICE_ENABLED = 1")
	}
}
