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

package extension

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// CredentialStore is the slice of milvus's account metadata a bootstrapper is
// allowed to touch. It is deliberately narrower than milvus's own catalog: a
// distribution seeding accounts needs to create them and bind them to roles,
// not to read back stored credentials.
//
// It does NOT create roles or grant privileges. milvus seeds roles from
// builtinRoles.roles, which creates each role and grants its privileges the
// same way this would have - so a form declares its roles there, as
// configuration, and this interface only has to attach an account to one.
//
// The method names are rootcoord's own (IMetaTable), so that a reader can
// find the operation behind each one without a translation table. milvus
// implements it; it may gain methods and never loses one.
type CredentialStore interface {
	// HasCredential reports whether a credential already exists for username.
	// A missing credential is not an error.
	HasCredential(ctx context.Context, username string) (bool, error)
	// AlterCredential stores an already-encrypted password for username. It
	// is an UPSERT, as its catalog namesake is: an existing credential is
	// overwritten without complaint, so a bootstrapper that must not reset a
	// password an operator changed checks HasCredential first. The caller
	// does the encryption; milvus does not see the plaintext.
	//
	// SEAM NOTE: the name is the catalog's (Catalog.AlterCredential), NOT
	// IMetaTable.AlterCredential. The latter takes a broadcast result and is
	// the WAL-replicated path the CreateCredential RPC uses; it needs the
	// streaming service, which is not up while rootcoord initializes, and
	// Bootstrap runs exactly then. The seam implements this method the way
	// rootcoord seeds its own root account at init - MetaTable.InitCredential:
	// Catalog.GetCredential to test presence, Catalog.AlterCredential to
	// write - and the two role methods below the way initRbac does, through
	// the MetaTable's direct OperateUserRole/SelectUser.
	AlterCredential(ctx context.Context, username, encryptedPassword string) error

	// OperateUserRole binds an account to a role that already exists. The
	// role is milvus's to create, from builtinRoles.roles.
	OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType) error
	// SelectUser reads an account's role bindings, so a bootstrapper can tell
	// a binding it already made from one it still has to.
	SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
}

// RBACBootstrapper seeds the accounts a deployment form needs before it serves
// traffic, and binds them to their roles.
//
// The ROLES themselves are not seeded here: a form declares them in
// builtinRoles.roles and milvus creates them, with their privileges, from that
// configuration. This runs after it has, so a role named below exists.
//
// It runs once during rootcoord initialization, single-threaded and before any
// request is accepted, so an implementation needs no locking of its own. It must
// be idempotent: rootcoord initializes on every restart.
//
// NoopRBACBootstrapper is the Noop base under the package evolution policy.
type RBACBootstrapper interface {
	// Bootstrap seeds accounts and binds them to roles. A non-nil error fails
	// rootcoord startup, because a form whose accounts are missing cannot
	// serve.
	Bootstrap(ctx context.Context, store CredentialStore) error
}

// NoopRBACBootstrapper seeds nothing, which is what a stock binary does.
type NoopRBACBootstrapper struct{}

var _ RBACBootstrapper = NoopRBACBootstrapper{}

func (NoopRBACBootstrapper) Bootstrap(context.Context, CredentialStore) error { return nil }
