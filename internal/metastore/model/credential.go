package model

import "github.com/milvus-io/milvus/pkg/proto/internalpb"

type Credential struct {
	Username          string
	EncryptedPassword string
	Tenant            string
	IsSuper           bool
	Sha256Password    string
}

func MarshalCredentialModel(cred *Credential) *internalpb.CredentialInfo {
	if cred == nil {
		return nil
	}
	return &internalpb.CredentialInfo{
		Tenant:            cred.Tenant,
		Username:          cred.Username,
		EncryptedPassword: cred.EncryptedPassword,
		IsSuper:           cred.IsSuper,
		Sha256Password:    cred.Sha256Password,
	}
}
