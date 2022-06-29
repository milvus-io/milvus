package crypto

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

func SHA256(src string, salt string) string {
	h := sha256.New()
	h.Write([]byte(src + salt))
	sum := h.Sum(nil)
	s := hex.EncodeToString(sum)

	return s
}

// PasswordEncrypt encrypt password
func PasswordEncrypt(pwd string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.MinCost)
	if err != nil {
		return "", err
	}

	return string(bytes), err
}

// PasswordVerify verify password
func PasswordVerify(rawPwd string, credInfo *internalpb.CredentialInfo) bool {
	// 1. hit cache
	if credInfo.Sha256Password != "" {
		encryped := SHA256(rawPwd, credInfo.Username)
		return encryped == credInfo.Sha256Password
	}

	// 2. miss cache, verify against encrypted password from etcd
	err := bcrypt.CompareHashAndPassword([]byte(credInfo.EncryptedPassword), []byte(rawPwd))
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
