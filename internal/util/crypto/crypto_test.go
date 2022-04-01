package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPasswordVerify(t *testing.T) {
	wrongPassword := "test_my_name"
	correctPassword := "test_my_pass_new"
	hashedPass, _ := PasswordEncrypt(correctPassword)
	assert.True(t, PasswordVerify(correctPassword, "$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2"))
	assert.False(t, PasswordVerify(wrongPassword, hashedPass))
}
