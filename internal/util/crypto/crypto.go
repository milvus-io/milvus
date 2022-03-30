package crypto

import (
	"encoding/base64"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

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
	if err != nil {
		log.Error("Verify password failed", zap.Error(err))
	}

	return err == nil
}

func Base64Decode(pwd string) (string, error) {
	bytes, err := base64.StdEncoding.DecodeString(pwd)
	if err != nil {
		return "", err
	}

	return string(bytes), err
}

func Base64Encode(pwd string) string {
	return base64.StdEncoding.EncodeToString([]byte(pwd))
}
