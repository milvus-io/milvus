package streamingutil

import (
	"os"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

const MilvusStreamingServiceEnabled = "MILVUS_STREAMING_SERVICE_ENABLED"

// IsStreamingServiceEnabled returns whether the streaming service is enabled.
func IsStreamingServiceEnabled() bool {
	// TODO: check if the environment variable MILVUS_STREAMING_SERVICE_ENABLED is set
	return os.Getenv(MilvusStreamingServiceEnabled) == "1"
}

// UseStreamingQueryNodeAsDelegator reports whether the query coordinator places
// shard delegators on streaming query nodes - the query node embedded in every
// streaming node - rather than on a replica's regular query nodes.
//
// With the streaming service on, that is what milvus does: a delegator lives
// beside the WAL it reads, a replica needs a streaming query node of its own,
// and a collection can hold no more replicas than there are streaming nodes.
//
// An installed form (extension.FormInstalled) keeps its streaming node for DDL
// and the write ahead log only. Its query clusters are resource groups of
// regular query nodes, several of which load the same collection, while the
// whole instance runs one streaming node; bound by the rule above, the second
// cluster's load would be refused. So under a form the delegators go where
// they went before the streaming service existed: onto the replica's regular
// RW query nodes, which watch the channel and read the WAL remotely. Every
// site that asks this already carries both placements; this only picks one.
func UseStreamingQueryNodeAsDelegator() bool {
	return IsStreamingServiceEnabled() && !extension.FormInstalled()
}

// SetStreamingServiceEnabled set the env that indicates whether the streaming service is enabled.
func SetStreamingServiceEnabled() {
	err := os.Setenv(MilvusStreamingServiceEnabled, "1")
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
