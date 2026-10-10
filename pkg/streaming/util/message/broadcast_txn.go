package message

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
