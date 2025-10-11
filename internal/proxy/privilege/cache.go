// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package privilege

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var cacheInst atomic.Pointer[privilegeCache]

func GetPrivilegeCache() *privilegeCache {
	return cacheInst.Load()
}

type PrivilegeCache interface {
	// GetCredentialInfo operate credential cache
	GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	RemoveCredential(username string)
	UpdateCredential(credInfo *internalpb.CredentialInfo)

	GetPrivilegeInfo(ctx context.Context) []string
	GetUserRole(username string) []string
	RefreshPolicyInfo(op typeutil.CacheOp) error
	InitPolicyInfo(info []string, userRoles []string)
}

var _ PrivilegeCache = (*privilegeCache)(nil)

type privilegeCache struct {
	mixCoord types.MixCoordClient

	mu             sync.RWMutex
	privilegeInfos map[string]struct{}            // privileges cache
	userToRoles    map[string]map[string]struct{} // user to role cache

	credMut sync.RWMutex
	credMap map[string]*internalpb.CredentialInfo
}

func InitPrivilegeCache(ctx context.Context, mixCoord types.MixCoordClient) error {
	privilegeCache := NewPrivilegeCache(mixCoord)
	// The privilege info is a little more. And to get this info, the query operation of involving multiple table queries is required.
	cacheInst.Store(privilegeCache)
	resp, err := mixCoord.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		log.Error("fail to init meta cache", zap.Error(err))
		return err
	}
	privilegeCache.InitPolicyInfo(resp.PolicyInfos, resp.UserRoles)
	log.Info("success to init privilege cache", zap.Strings("policy_infos", resp.PolicyInfos))
	return nil
}

func NewPrivilegeCache(mixCoord types.MixCoordClient) *privilegeCache {
	return &privilegeCache{
		mixCoord:       mixCoord,
		privilegeInfos: make(map[string]struct{}),
		userToRoles:    make(map[string]map[string]struct{}),

		credMap: make(map[string]*internalpb.CredentialInfo),
	}
}

// GetCredentialInfo returns the credential related to provided username
// If the cache missed, proxy will try to fetch from storage
func (m *privilegeCache) GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error) {
	m.credMut.RLock()
	var credInfo *internalpb.CredentialInfo
	credInfo, ok := m.credMap[username]
	m.credMut.RUnlock()

	if !ok {
		req := &rootcoordpb.GetCredentialRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_GetCredential),
			),
			Username: username,
		}
		resp, err := m.mixCoord.GetCredential(ctx, req)
		if err != nil {
			return &internalpb.CredentialInfo{}, err
		}
		credInfo = &internalpb.CredentialInfo{
			Username:          resp.Username,
			EncryptedPassword: resp.Password,
		}
	}

	return credInfo, nil
}

func (m *privilegeCache) RemoveCredential(username string) {
	m.credMut.Lock()
	defer m.credMut.Unlock()
	// delete pair in credMap
	delete(m.credMap, username)
}

func (m *privilegeCache) UpdateCredential(credInfo *internalpb.CredentialInfo) {
	m.credMut.Lock()
	defer m.credMut.Unlock()
	username := credInfo.Username
	_, ok := m.credMap[username]
	if !ok {
		m.credMap[username] = &internalpb.CredentialInfo{}
	}

	// Do not cache encrypted password content
	m.credMap[username].Username = username
	m.credMap[username].Sha256Password = credInfo.Sha256Password
}

func (m *privilegeCache) InitPolicyInfo(info []string, userRoles []string) {
	defer func() {
		err := GetEnforcer().LoadPolicy()
		if err != nil {
			log.Error("failed to load policy after RefreshPolicyInfo", zap.Error(err))
		}
		CleanPrivilegeCache()
	}()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsafeInitPolicyInfo(info, userRoles)
}

func (m *privilegeCache) unsafeInitPolicyInfo(info []string, userRoles []string) {
	m.privilegeInfos = util.StringSet(info)
	for _, userRole := range userRoles {
		user, role, err := funcutil.DecodeUserRoleCache(userRole)
		if err != nil {
			log.Warn("invalid user-role key", zap.String("user-role", userRole), zap.Error(err))
			continue
		}
		if m.userToRoles[user] == nil {
			m.userToRoles[user] = make(map[string]struct{})
		}
		m.userToRoles[user][role] = struct{}{}
	}
}

func (m *privilegeCache) GetPrivilegeInfo(ctx context.Context) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return util.StringList(m.privilegeInfos)
}

func (m *privilegeCache) GetUserRole(user string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return util.StringList(m.userToRoles[user])
}

func (m *privilegeCache) RefreshPolicyInfo(op typeutil.CacheOp) (err error) {
	defer func() {
		if err == nil {
			le := GetEnforcer().LoadPolicy()
			if le != nil {
				log.Error("failed to load policy after RefreshPolicyInfo", zap.Error(le))
			}
			CleanPrivilegeCache()
		}
	}()
	if op.OpType != typeutil.CacheRefresh {
		m.mu.Lock()
		defer m.mu.Unlock()
		if op.OpKey == "" {
			return errors.New("empty op key")
		}
	}

	switch op.OpType {
	case typeutil.CacheGrantPrivilege:
		keys := funcutil.PrivilegesForPolicy(op.OpKey)
		for _, key := range keys {
			m.privilegeInfos[key] = struct{}{}
		}
	case typeutil.CacheRevokePrivilege:
		keys := funcutil.PrivilegesForPolicy(op.OpKey)
		for _, key := range keys {
			delete(m.privilegeInfos, key)
		}
	case typeutil.CacheAddUserToRole:
		user, role, err := funcutil.DecodeUserRoleCache(op.OpKey)
		if err != nil {
			return fmt.Errorf("invalid opKey, fail to decode, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
		}
		if m.userToRoles[user] == nil {
			m.userToRoles[user] = make(map[string]struct{})
		}
		m.userToRoles[user][role] = struct{}{}
	case typeutil.CacheRemoveUserFromRole:
		user, role, err := funcutil.DecodeUserRoleCache(op.OpKey)
		if err != nil {
			return fmt.Errorf("invalid opKey, fail to decode, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
		}
		if m.userToRoles[user] != nil {
			delete(m.userToRoles[user], role)
		}
	case typeutil.CacheDeleteUser:
		delete(m.userToRoles, op.OpKey)
	case typeutil.CacheDropRole:
		for user := range m.userToRoles {
			delete(m.userToRoles[user], op.OpKey)
		}

		for policy := range m.privilegeInfos {
			if funcutil.PolicyCheckerWithRole(policy, op.OpKey) {
				delete(m.privilegeInfos, policy)
			}
		}
	case typeutil.CacheRefresh:
		resp, err := m.mixCoord.ListPolicy(context.Background(), &internalpb.ListPolicyRequest{})
		if err != nil {
			log.Error("fail to init meta cache", zap.Error(err))
			return err
		}

		if !merr.Ok(resp.GetStatus()) {
			log.Error("fail to init meta cache",
				zap.String("error_code", resp.GetStatus().GetErrorCode().String()),
				zap.String("reason", resp.GetStatus().GetReason()))
			return merr.Error(resp.Status)
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		m.userToRoles = make(map[string]map[string]struct{})
		m.privilegeInfos = make(map[string]struct{})
		m.unsafeInitPolicyInfo(resp.PolicyInfos, resp.UserRoles)
	default:
		return fmt.Errorf("invalid opType, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
	}
	return nil
}
