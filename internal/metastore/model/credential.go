package model

import "github.com/milvus-io/milvus/pkg/v2/proto/internalpb"

type Credential struct {
	Username          string
	EncryptedPassword string
	Tenant            string
	IsSuper           bool
	Sha256Password    string
	TimeTick          uint64 // the timetick in wal which the credential updates
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
		TimeTick:          cred.TimeTick,
	}
}

func UnmarshalCredentialModel(cred *internalpb.CredentialInfo) *Credential {
	if cred == nil {
		return nil
	}
	return &Credential{
		Username:          cred.Username,
		EncryptedPassword: cred.EncryptedPassword,
		Tenant:            cred.Tenant,
		IsSuper:           cred.IsSuper,
		Sha256Password:    cred.Sha256Password,
		TimeTick:          cred.TimeTick,
	}
}
