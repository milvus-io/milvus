package crypto

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestPasswordEncryptUsesDefaultBcryptCost(t *testing.T) {
	encryptedPassword, err := PasswordEncrypt("test-password")
	require.NoError(t, err)

	cost, err := bcrypt.Cost([]byte(encryptedPassword))
	require.NoError(t, err)
	assert.Equal(t, bcrypt.DefaultCost, cost)
	assert.GreaterOrEqual(t, cost, 10)
}

func TestPasswordEncryptVerification(t *testing.T) {
	password := "test-password"
	encryptedPassword, err := PasswordEncrypt(password)
	require.NoError(t, err)

	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(encryptedPassword), []byte(password)))
	assert.Error(t, bcrypt.CompareHashAndPassword([]byte(encryptedPassword), []byte("wrong-password")))
}

func TestPasswordEncryptSupportsLegacyBcryptHash(t *testing.T) {
	const legacyHash = "$2a$04$FDqkwIegT7SfTGRcPyQjk.reG7poJ99Two1jsl2euNBy0Z.e1DiR."

	cost, err := bcrypt.Cost([]byte(legacyHash))
	require.NoError(t, err)
	assert.Equal(t, bcrypt.MinCost, cost)
	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(legacyHash), []byte("legacy-password")))
}

func BenchmarkPasswordHashGeneration(b *testing.B) {
	benchmarkBcryptCosts(b, func(b *testing.B, cost int) {
		for i := 0; i < b.N; i++ {
			if _, err := bcrypt.GenerateFromPassword([]byte("benchmark-password"), cost); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPasswordHashComparison(b *testing.B) {
	benchmarkBcryptCosts(b, func(b *testing.B, cost int) {
		hash, err := bcrypt.GenerateFromPassword([]byte("benchmark-password"), cost)
		require.NoError(b, err)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := bcrypt.CompareHashAndPassword(hash, []byte("benchmark-password")); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPasswordHashComparisonParallel(b *testing.B) {
	benchmarkBcryptCosts(b, func(b *testing.B, cost int) {
		hash, err := bcrypt.GenerateFromPassword([]byte("benchmark-password"), cost)
		require.NoError(b, err)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := bcrypt.CompareHashAndPassword(hash, []byte("benchmark-password")); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func benchmarkBcryptCosts(b *testing.B, benchmark func(b *testing.B, cost int)) {
	b.Helper()
	for _, cost := range []int{bcrypt.MinCost, bcrypt.DefaultCost} {
		b.Run(fmt.Sprintf("cost_%d", cost), func(b *testing.B) {
			b.ReportAllocs()
			benchmark(b, cost)
		})
	}
}

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
