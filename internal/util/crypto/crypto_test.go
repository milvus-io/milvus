package crypto

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/stretchr/testify/assert"
)

func TestPasswordVerify(t *testing.T) {
	wrongPassword := "test_my_name"
	correctPassword := "test_my_pass_new"
	hashedPass, _ := PasswordEncrypt(correctPassword)
	assert.True(t, PasswordVerify(correctPassword, "$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2"))
	assert.False(t, PasswordVerify(wrongPassword, hashedPass))
}

func TestMarshalAndPasswordVerify(t *testing.T) {
	encryptedRootPassword, _ := PasswordEncrypt(util.DefaultRootPassword)
	credInfo := &internalpb.CredentialInfo{Username: util.UserRoot, EncryptedPassword: encryptedRootPassword}
	v, _ := proto.Marshal(&internalpb.CredentialInfo{EncryptedPassword: credInfo.EncryptedPassword})
	fmt.Println(string(v))

	credentialInfo := internalpb.CredentialInfo{}
	proto.Unmarshal(v, &credentialInfo)
	assert.True(t, PasswordVerify(util.DefaultRootPassword, credentialInfo.EncryptedPassword))
}

func TestJsonMarshalAndPasswordVerify(t *testing.T) {
	encryptedRootPassword, _ := PasswordEncrypt(util.DefaultRootPassword)
	credInfo := &internalpb.CredentialInfo{Username: util.UserRoot, EncryptedPassword: encryptedRootPassword}
	v, _ := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: credInfo.EncryptedPassword})
	fmt.Println(string(v))

	credentialInfo := internalpb.CredentialInfo{}
	json.Unmarshal(v, &credentialInfo)
	assert.True(t, PasswordVerify(util.DefaultRootPassword, credentialInfo.EncryptedPassword))
}
