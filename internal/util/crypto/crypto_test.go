package crypto

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/bcrypt"
)

func TestPasswordVerify_HitCache(t *testing.T) {
	wrongPassword := "test_my_name"
	correctPassword := "test_my_pass_new"
	credInfo := &internalpb.CredentialInfo{
		Username:       "root",
		Sha256Password: "bcca79df9650cef1d7ed9f63449d7f8a27843d2678f5666f38ca65ab77d99a13",
	}
	assert.True(t, PasswordVerify(correctPassword, credInfo))
	assert.False(t, PasswordVerify(wrongPassword, credInfo))
}

func TestPasswordVerify_MissCache(t *testing.T) {
	wrongPassword := "test_my_name"
	correctPassword := "test_my_pass_new"
	credInfo := &internalpb.CredentialInfo{
		EncryptedPassword: "$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2",
	}
	assert.True(t, PasswordVerify(correctPassword, credInfo))
	assert.False(t, PasswordVerify(wrongPassword, credInfo))
}

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
