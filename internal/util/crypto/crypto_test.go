package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPasswordVerify(t *testing.T) {
	wrongPassword := "test_my_name"
	correctPassword := "test_my_pass_new"
	//hashedPassword := "$2a$10$ejSpVKQKF6stPZzcTmlPCuaWONZYiE2KEtqaMxTNs8up12RnPj41O"
	hashedPassword := "$2a$10$ywLvZEbTecnq2ABwmPZJWOAcq9CvT7g2upHT8.SIpBkdxRBKwMvgy"
	assert.True(t, PasswordVerify(correctPassword, hashedPassword))
	assert.False(t, PasswordVerify(wrongPassword, hashedPassword))
}
