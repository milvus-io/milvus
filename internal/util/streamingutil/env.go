package streamingutil

import "os"

const MilvusStreamingServiceEnabled = "MILVUS_STREAMING_SERVICE_ENABLED"

// IsStreamingServiceEnabled returns whether the streaming service is enabled.
func IsStreamingServiceEnabled() bool {
	// TODO: check if the environment variable MILVUS_STREAMING_SERVICE_ENABLED is set
	return os.Getenv(MilvusStreamingServiceEnabled) == "1"
}

// MustEnableStreamingService panics if the streaming service is not enabled.
func MustEnableStreamingService() {
	if !IsStreamingServiceEnabled() {
		panic("start a streaming node without enabling streaming service, please set environment variable MILVUS_STREAMING_SERVICE_ENABLED = 1")
	}
}
