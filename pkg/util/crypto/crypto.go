package crypto

import (
	"crypto/md5" // #nosec
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Values of common.security.passwordHashAlgorithm.
const (
	PasswordHashBcrypt       = "bcrypt"
	PasswordHashPBKDF2SHA512 = "pbkdf2-sha512" //nolint:gosec // algorithm name, not a credential
)

func SHA256(src string, salt string) string {
	h := sha256.New()
	h.Write([]byte(src + salt))
	sum := h.Sum(nil)
	s := hex.EncodeToString(sum)

	return s
}

// PasswordEncrypt hashes a password with the algorithm selected by
// common.security.passwordHashAlgorithm. An unrecognized value is an error
// rather than a fallback, so that a typo cannot quietly keep producing bcrypt
// hashes on a deployment that asked for something else.
func PasswordEncrypt(pwd string) (string, error) {
	switch algorithm := paramtable.Get().CommonCfg.PasswordHashAlgorithm.GetValue(); algorithm {
	case PasswordHashBcrypt:
		return bcryptHash(pwd)
	case PasswordHashPBKDF2SHA512:
		return encryptPBKDF2SHA512(pwd)
	default:
		return "", merr.WrapErrServiceInternalMsg("unsupported common.security.passwordHashAlgorithm %q", algorithm)
	}
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

func MD5(str string) string {
	// #nosec
	data := md5.Sum([]byte(str))
	return hex.EncodeToString(data[:])[8:24]
}

func GranteeID(str string) string {
	// #nosec G401 -- RBAC grantee IDs need stable 128-bit identifiers, not password hashing.
	data := md5.Sum([]byte(str))
	return hex.EncodeToString(data[:])
}
