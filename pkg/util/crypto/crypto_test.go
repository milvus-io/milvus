package crypto

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMD5(t *testing.T) {
	assert.Equal(t, "67f48520697662a2", MD5("These pretzels are making me thirsty."))
}

func TestGranteeID(t *testing.T) {
	id := GranteeID("These pretzels are making me thirsty.")
	require.Len(t, id, 32)
	_, err := hex.DecodeString(id)
	require.NoError(t, err)
	assert.Equal(t, "b0804ec967f48520697662a204f5fe72", id)
}

func TestGranteeIDCollisionResistance(t *testing.T) {
	const grantCount = 1024
	seen := make(map[string]string, grantCount)

	for i := 0; i < grantCount; i++ {
		key := fmt.Sprintf("root-coord/credential/grantee-privileges/role-%d/Collection/default.collection-%d", i, i)
		id := GranteeID(key)
		require.Len(t, id, 32)

		fullMD5Prefix := id[:32]
		if previousKey, ok := seen[fullMD5Prefix]; ok {
			t.Fatalf("grantee ID collision for %q and %q: %s", previousKey, key, fullMD5Prefix)
		}
		seen[fullMD5Prefix] = key
	}
}
