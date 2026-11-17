// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package credentials

import (
	"strings"
	"time"
)

// Snapshot is one atomic credential generation. Expiration is the absolute
// timestamp returned by the credential service. Static is true only for
// explicitly configured access_key/sts credentials, whose lifetime is unknown.
type Snapshot struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
	Expiration      time.Time
	Static          bool
}

func completeSession(c *sessionCredential, requireToken bool) bool {
	return c != nil && strings.TrimSpace(c.AccessKeyId) != "" && strings.TrimSpace(c.AccessKeySecret) != "" &&
		(!requireToken || strings.TrimSpace(c.SecurityToken) != "")
}

func validateSession(c *sessionCredential, expiry string, requireToken bool, now time.Time) (time.Time, error) {
	if !completeSession(c, requireToken) {
		return time.Time{}, newCredentialError("credential endpoint returned incomplete credentials")
	}
	expiration, err := time.Parse(time.RFC3339, strings.TrimSpace(expiry))
	if err != nil || !expiration.After(now) {
		return time.Time{}, newCredentialError("credential endpoint returned invalid or expired credentials")
	}
	return expiration, nil
}

func sessionSnapshot(u *credentialUpdater, current func() *sessionCredential, refresh func() error, requireToken bool) (Snapshot, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	var refreshErr error
	if current() == nil || u.needUpdateCredential() {
		refreshErr = refresh()
	}
	// A failed refresh can finish after the old credentials actually expire.
	// Check completion time before serving either generation.
	c := current()
	if !completeSession(c, requireToken) || !u.expiresAt.After(u.timeNow()) {
		if refreshErr != nil {
			return Snapshot{}, wrapCredentialError(refreshErr, "refresh Aliyun credentials")
		}
		return Snapshot{}, newCredentialError("no unexpired Aliyun credentials available")
	}
	return Snapshot{
		AccessKeyId: c.AccessKeyId, AccessKeySecret: c.AccessKeySecret,
		SecurityToken: c.SecurityToken, Expiration: u.expiresAt,
	}, nil
}

// RetrieveSnapshot retains the SDK's chosen identity and does not attempt a
// different provider on refresh failure. All refreshable providers return an
// authoritative expiration and retain only fully validated generations.
func RetrieveSnapshot(credential Credential) (Snapshot, error) {
	switch c := credential.(type) {
	case *OIDCCredential:
		return sessionSnapshot(c.credentialUpdater, func() *sessionCredential { return c.sessionCredential }, c.updateCredential, true)
	case *RAMRoleArnCredential:
		return sessionSnapshot(c.credentialUpdater, func() *sessionCredential { return c.sessionCredential }, c.updateCredential, true)
	case *EcsRAMRoleCredential:
		return sessionSnapshot(c.credentialUpdater, func() *sessionCredential { return c.sessionCredential }, c.updateCredential, true)
	case *URLCredential:
		return sessionSnapshot(c.credentialUpdater, func() *sessionCredential { return c.sessionCredential }, c.updateCredential, true)
	case *RsaKeyPairCredential:
		return sessionSnapshot(c.credentialUpdater, func() *sessionCredential { return c.sessionCredential }, c.updateCredential, false)
	}
	// Preserve structural compatibility for explicit static credentials and
	// existing mocks; unknown dynamic lifetimes must not be invented.
	if credential == nil || credential.GetType() == nil {
		return Snapshot{}, newCredentialError("missing Aliyun credential type")
	}
	kind := *credential.GetType()
	if kind != "access_key" && kind != "sts" {
		return Snapshot{}, newCredentialError("Aliyun credential type does not expose an atomic expiration-bearing snapshot")
	}
	ak, err := credential.GetAccessKeyId()
	if err != nil {
		return Snapshot{}, err
	}
	sk, err := credential.GetAccessKeySecret()
	if err != nil {
		return Snapshot{}, err
	}
	token, err := credential.GetSecurityToken()
	if err != nil {
		return Snapshot{}, err
	}
	if ak == nil || sk == nil || token == nil {
		return Snapshot{}, newCredentialError("incomplete static Aliyun credentials")
	}
	value := sessionCredential{AccessKeyId: *ak, AccessKeySecret: *sk, SecurityToken: *token}
	if !completeSession(&value, kind == "sts") {
		return Snapshot{}, newCredentialError("incomplete static Aliyun credentials")
	}
	return Snapshot{AccessKeyId: *ak, AccessKeySecret: *sk, SecurityToken: *token, Static: true}, nil
}
