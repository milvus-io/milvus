package registry

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

var (
	longLockOwnershipKeyParsersMu sync.RWMutex
	longLockOwnershipKeyParsers   = make(map[message.MessageTypeWithVersion]func(message.BroadcastMutableMessage) (string, bool))
)

// RegisterLongLockOwnershipKeyParser registers a pure message parser during package init,
// before broadcaster recovery or admission starts. The parser returns a stable,
// business-namespaced key shared by the long-lock owner and its terminal messages,
// and whether this message is the owner. An empty key uses ordinary locking.
// Unlike server-bound ACK callbacks, these static parsers survive test resets.
func RegisterLongLockOwnershipKeyParser(typ message.MessageTypeWithVersion, parser func(message.BroadcastMutableMessage) (ownershipKey string, isOwner bool)) {
	longLockOwnershipKeyParsersMu.Lock()
	defer longLockOwnershipKeyParsersMu.Unlock()
	longLockOwnershipKeyParsers[typ] = parser
}

// ParseLongLockOwnershipKey resolves long-lock ownership without interpreting business messages.
// Unregistered message types retain the single-broadcast lock lifecycle.
func ParseLongLockOwnershipKey(msg message.BroadcastMutableMessage) (ownershipKey string, isOwner bool) {
	longLockOwnershipKeyParsersMu.RLock()
	parser := longLockOwnershipKeyParsers[msg.MessageTypeWithVersion()]
	longLockOwnershipKeyParsersMu.RUnlock()
	if parser == nil {
		return "", false
	}
	return parser(msg)
}
