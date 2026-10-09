package message

import (
	"bytes"
	"maps"
	"slices"
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

const broadcastAdmissionKey = "_bik"

// NewIdempotencyResourceKey names an operation and its scoped client key.
// Its exclusive lock is acquired BEFORE business resources by the txn API.
// Empty keys disable admission deduplication. Operation must be nonempty.
func NewIdempotencyResourceKey(operation string, key IdempotencyKey) ResourceKey {
	value := ""
	if operation != "" && key != "" {
		value = strconv.Itoa(len(operation)) + ":" + operation + string(key)
	}
	return ResourceKey{Domain: messagespb.ResourceDomain_ResourceDomainIdempotency, Key: value}
}

// BroadcastAdmissionKeyOf returns the complete persisted admission identity.
// Unlike IdempotencyKeyOf, this identity already includes the operation domain.
func BroadcastAdmissionKeyOf(msg BasicMessage) string {
	key, _ := msg.Properties().Get(broadcastAdmissionKey)
	return key
}

// SameBroadcastOperation compares business content, excluding framework IDs and tracing.
// ACK requirements and data channels remain part of the operation's identity.
func SameBroadcastOperation(a, b BroadcastMutableMessage) bool {
	if !bytes.Equal(a.Payload(), b.Payload()) {
		return false
	}
	normalize := func(msg BroadcastMutableMessage) map[string]string {
		p := maps.Clone(msg.Properties().ToRawMap())
		delete(p, messageBroadcastHeader)
		delete(p, messageTraceContext)
		return p
	}
	channels := func(msg BroadcastMutableMessage) []string {
		result := slices.Clone(msg.BroadcastHeader().VChannels)
		result = slices.DeleteFunc(result, funcutil.IsControlChannel)
		slices.Sort(result)
		return result
	}
	return a.BroadcastHeader().AckSyncUp == b.BroadcastHeader().AckSyncUp &&
		slices.Equal(channels(a), channels(b)) && maps.Equal(normalize(a), normalize(b))
}
