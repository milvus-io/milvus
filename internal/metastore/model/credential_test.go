package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

var (
	credentialModel = &Credential{
		Username:          "user",
		EncryptedPassword: "password",
		Tenant:            "tenant-1",
		IsSuper:           true,
		Sha256Password:    "xxxx",
		Description:       "description",
	}

	description  = "description"
	credentialPb = &internalpb.CredentialInfo{
		Username:          "user",
		EncryptedPassword: "password",
		Tenant:            "tenant-1",
		IsSuper:           true,
		Sha256Password:    "xxxx",
		Description:       &description,
	}
)

func TestMarshalCredentialModel(t *testing.T) {
	ret := MarshalCredentialModel(credentialModel)
	assert.Equal(t, credentialPb, ret)

	assert.Nil(t, MarshalCredentialModel(nil))
}
