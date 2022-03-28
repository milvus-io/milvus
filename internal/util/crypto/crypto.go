package crypto

import (
	"github.com/dvsekhvalnov/jose2go/base64url"
	"golang.org/x/crypto/bcrypt"
)

// PasswordEncrypt encrypt password
func PasswordEncrypt(pwd string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	return string(bytes), err
}

// PasswordVerify verify encrypted password
func PasswordVerify(pwd, hashPwd string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashPwd), []byte(pwd))

	return err == nil
}

func Base64Decode(pwd string) (string, error) {
	bytes, err := base64url.Decode(pwd)
	if err != nil {
		return "", err
	}

	return string(bytes), err
}
