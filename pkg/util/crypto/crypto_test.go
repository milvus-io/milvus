package crypto

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

//func BenchmarkPasswordVerify(b *testing.B) {
//	correctPassword := "test_my_pass_new"
//	credInfo := &internalpb.CredentialInfo{
//		Username:       "root",
//		Sha256Password: "bcca79df9650cef1d7ed9f63449d7f8a27843d2678f5666f38ca65ab77d99a13",
//	}
//	b.ResetTimer()
//	for n := 0; n < b.N; n++ {
//		PasswordVerify(correctPassword, credInfo)
//	}
//}

func TestBcryptCompare(t *testing.T) {
	wrongPassword := "test_my_name"
	correctPassword := "test_my_pass_new"

	err := bcrypt.CompareHashAndPassword([]byte("$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2"), []byte(correctPassword))
	assert.NoError(t, err)

	err = bcrypt.CompareHashAndPassword([]byte("$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2"), []byte(wrongPassword))
	assert.Error(t, err)
}

func TestBcryptCost(t *testing.T) {
	correctPassword := "test_my_pass_new"

	bytes, _ := bcrypt.GenerateFromPassword([]byte(correctPassword), bcrypt.DefaultCost)
	err := bcrypt.CompareHashAndPassword(bytes, []byte(correctPassword))
	assert.NoError(t, err)

	bytes, _ = bcrypt.GenerateFromPassword([]byte(correctPassword), bcrypt.MinCost)
	err = bcrypt.CompareHashAndPassword(bytes, []byte(correctPassword))
	assert.NoError(t, err)
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
