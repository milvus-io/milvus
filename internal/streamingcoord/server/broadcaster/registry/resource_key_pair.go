package registry

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

var (
	resourceKeyPairsMu sync.RWMutex
	resourceKeyPairs   = make(map[message.MessageTypeWithVersion]func(message.BroadcastMutableMessage) (string, bool))
)

// RegisterResourceKeyPair registers a pure message parser during package init,
// before broadcaster recovery or admission starts. The parser returns a stable,
// business-namespaced key shared by the owner and its terminal messages, and
// whether this message acquires ownership. An empty key uses ordinary locking.
// Unlike server-bound ACK callbacks, these static parsers survive test resets.
func RegisterResourceKeyPair(typ message.MessageTypeWithVersion, parser func(message.BroadcastMutableMessage) (key string, owner bool)) {
	resourceKeyPairsMu.Lock()
	defer resourceKeyPairsMu.Unlock()
	resourceKeyPairs[typ] = parser
}

// ResourceKeyPair resolves ownership without interpreting business messages.
// Unregistered message types retain the single-broadcast lock lifecycle.
func ResourceKeyPair(msg message.BroadcastMutableMessage) (key string, owner bool) {
	resourceKeyPairsMu.RLock()
	parser := resourceKeyPairs[msg.MessageTypeWithVersion()]
	resourceKeyPairsMu.RUnlock()
	if parser == nil {
		return "", false
	}
	return parser(msg)
}
