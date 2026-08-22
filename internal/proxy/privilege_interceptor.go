package proxy

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type PrivilegeFunc func(ctx context.Context, req interface{}) (context.Context, error)

const RBACRoleContextKey = hook.HookContextKeyType("rbac-role")

var (
	initOnce                sync.Once
	initPrivilegeGroupsOnce sync.Once
)

var roPrivileges, rwPrivileges, adminPrivileges map[string]struct{}

func SetRBACRolesToContext(ctx context.Context, roles []string) context.Context {
	rolesCopy := append([]string(nil), roles...)
	return context.WithValue(ctx, RBACRoleContextKey, rolesCopy)
}

// UnaryServerInterceptor returns a new unary server interceptors that performs per-request privilege access.
func UnaryServerInterceptor(privilegeFunc PrivilegeFunc) grpc.UnaryServerInterceptor {
	privilege.InitPrivilegeGroups()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := privilegeFunc(ctx, req)
		if err != nil {
			hookutil.GetExtension().ReportAction(newCtx, req, &milvuspb.BoolResponse{
				Status: merr.Status(err),
			}, err, info.FullMethod, hookutil.ActionAuthorize)
			return nil, err
		}
		return handler(newCtx, req)
	}
}

func PrivilegeInterceptor(ctx context.Context, req interface{}) (context.Context, error) {
	return PrivilegeInterceptorWithMetaCache(func() Cache { return nil })(ctx, req)
}

func PrivilegeInterceptorWithMetaCache(getMetaCache func() Cache) PrivilegeFunc {
	return func(ctx context.Context, req interface{}) (context.Context, error) {
		if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
			return ctx, nil
		}
		mlog.RatedDebug(ctx, rate.Limit(60), "PrivilegeInterceptor", mlog.String("type", reflect.TypeOf(req).String()))
		privilegeExt, err := funcutil.GetPrivilegeExtObj(req)
		if err != nil {
			mlog.RatedInfo(ctx, rate.Limit(60), "GetPrivilegeExtObj err", mlog.Err(err))
			return ctx, nil
		}
		username, password, err := contextutil.GetAuthInfoFromContext(ctx)
		if err != nil {
			mlog.Warn(ctx, "GetCurUserFromContext fail", mlog.Err(err))
			return ctx, err
		}
		if !Params.CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
			return ctx, nil
		}
		roleNames, err := GetRole(username)
		if err != nil {
			mlog.Warn(ctx, "GetRole fail", mlog.String("username", username), mlog.Err(err))
			return ctx, err
		}
		roleNames = append(roleNames, util.RolePublic)
		ctx = SetRBACRolesToContext(ctx, roleNames)
		objectType := privilegeExt.ObjectType.String()
		objectNameIndex := privilegeExt.ObjectNameIndex
		objectName := funcutil.GetObjectName(req, objectNameIndex)
		objectPrivilege := privilegeExt.ObjectPrivilege.String()
		// Resolve resources against the database the request actually reads from,
		// while keeping the database used by the policy check separate. Alias
		// resolution must remain database-scoped even when a cross-database
		// operation requires a cluster-scoped policy (db="*").
		//
		// Policy scope mirrors the grant-side validation (see milvus-io/milvus#50678):
		//   - Cluster-level privileges (CreateDatabase/ResourceGroup/...) are not
		//     scoped to a database, so authorize them globally (AnyWord),
		//     independent of the connection namespace.
		//   - Database-/Collection-level privileges are scoped to the db the request
		//     targets: the request-body DbName takes precedence, falling back to the
		//     connection-context db.
		dbName := GetCurDBNameFromRequestOrContext(ctx, req)
		policyDBName := dbName
		if util.GetPrivilegeLevel(util.MetaStore2API(objectPrivilege)) == milvuspb.PrivilegeLevel_Cluster.String() {
			policyDBName = util.AnyWord
		}
		// RenameCollection is a database-admin privilege: a same-db rename is
		// authorized against the target db (database level, handled above), while a
		// cross-db rename additionally requires a cluster-scoped (global) grant.
		if r, ok := req.(*milvuspb.RenameCollectionRequest); ok && r.GetDbName() != r.GetNewDBName() {
			policyDBName = util.AnyWord
		}
		// RestoreSnapshot is collection-scoped within one database. Restoring into
		// another database creates a collection there, so require the same privilege
		// at cluster scope (db="*") instead of authorizing only against the source.
		if r, ok := req.(*milvuspb.RestoreSnapshotRequest); ok {
			targetDBName := r.GetTargetDbName()
			if targetDBName == "" {
				targetDBName = GetCurDBNameFromContextOrDefault(ctx)
			}
			if dbName != targetDBName {
				policyDBName = util.AnyWord
			}
		}

		// Resolve alias to actual collection name for RBAC checks
		if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() && objectType == commonpb.ObjectType_Collection.String() && objectNameIndex != 0 {
			if objectName != util.AnyWord && objectName != "" {
				if actualCollectionName, resolveErr := resolveCollectionAlias(ctx, getMetaCache(), dbName, objectName); resolveErr != nil {
					mlog.RatedWarn(ctx, rate.Limit(60), "failed to resolve collection alias for RBAC, using original name",
						mlog.String("objectName", objectName), mlog.FieldDbName(dbName), mlog.Err(resolveErr))
				} else {
					objectName = actualCollectionName
				}
			}
		}

		if isCurUserObject(objectType, username, objectName) {
			return ctx, nil
		}

		if isSelectMyRoleGrants(req, roleNames) {
			return ctx, nil
		}

		objectNameIndexs := privilegeExt.ObjectNameIndexs
		objectNames := funcutil.GetObjectNames(req, objectNameIndexs)

		// Resolve aliases for operations that refer to multiple resources
		if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() && objectType == commonpb.ObjectType_Collection.String() && objectNameIndexs != 0 && len(objectNames) > 0 {
			resolvedNames := make([]string, 0, len(objectNames))
			for _, name := range objectNames {
				if name == util.AnyWord || name == "" {
					resolvedNames = append(resolvedNames, name)
					continue
				}
				if actualName, resolveErr := resolveCollectionAlias(ctx, getMetaCache(), dbName, name); resolveErr != nil {
					mlog.RatedWarn(ctx, rate.Limit(60), "failed to resolve collection alias for RBAC, using original name",
						mlog.String("objectName", name), mlog.FieldDbName(dbName), mlog.Err(resolveErr))
					resolvedNames = append(resolvedNames, name)
				} else {
					resolvedNames = append(resolvedNames, actualName)
				}
			}
			objectNames = resolvedNames
		}

		log := mlog.With(mlog.String("username", username), mlog.Strings("role_names", roleNames),
			mlog.String("object_type", objectType), mlog.String("object_privilege", objectPrivilege),
			mlog.FieldDbName(policyDBName),
			mlog.Int32("object_index", objectNameIndex), mlog.String("object_name", objectName),
			mlog.Int32("object_indexs", objectNameIndexs), mlog.Strings("object_names", objectNames))

		e := privilege.GetEnforcer()
		for _, roleName := range roleNames {
			permitFunc := func(objectName string) (bool, error) {
				object := funcutil.PolicyForResource(policyDBName, objectType, objectName)
				isPermit, cached, version := privilege.GetResultCache(roleName, object, objectPrivilege)
				if cached {
					return isPermit, nil
				}
				isPermit, err := e.Enforce(roleName, object, objectPrivilege)
				if err != nil {
					return false, err
				}
				privilege.SetResultCache(roleName, object, objectPrivilege, isPermit, version)
				return isPermit, nil
			}

			if objectNameIndex != 0 {
				// handle the api which refers one resource
				permitObject, err := permitFunc(objectName)
				if err != nil {
					log.Warn(ctx, "fail to execute permit func", mlog.String("name", objectName), mlog.Err(err))
					return ctx, err
				}
				if permitObject {
					return ctx, nil
				}
			}

			if objectNameIndexs != 0 {
				// handle the api which refers many resources
				permitObjects := true
				for _, name := range objectNames {
					p, err := permitFunc(name)
					if err != nil {
						log.Warn(ctx, "fail to execute permit func", mlog.String("name", name), mlog.Err(err))
						return ctx, err
					}
					if !p {
						permitObjects = false
						break
					}
				}
				if permitObjects && len(objectNames) != 0 {
					return ctx, nil
				}
			}
		}

		log.Info(ctx, "permission deny", mlog.Strings("roles", roleNames))

		if password == util.PasswordHolder {
			username = "apikey user"
		}

		return ctx, status.Error(codes.PermissionDenied,
			fmt.Sprintf("%s: permission deny to %s in the `%s` database", objectPrivilege, username, policyDBName))
	}
}

// isCurUserObject Determine whether it is an Object of type User that operates on its own user information,
// like updating password or viewing your own role information.
// make users operate their own user information when the related privileges are not granted.
func isCurUserObject(objectType string, curUser string, object string) bool {
	if objectType != commonpb.ObjectType_User.String() {
		return false
	}
	return curUser == object
}

func isSelectMyRoleGrants(req interface{}, roleNames []string) bool {
	selectGrantReq, ok := req.(*milvuspb.SelectGrantRequest)
	if !ok {
		return false
	}
	filterGrantEntity := selectGrantReq.GetEntity()
	roleName := filterGrantEntity.GetRole().GetName()
	return funcutil.SliceContain(roleNames, roleName)
}

func checkSkipRLSPrivilege(ctx context.Context, metaCache Cache, dbName, collectionName, operation string) error {
	permitted, err := isCurrentUserPermitted(ctx, metaCache, dbName, commonpb.ObjectType_Collection.String(), collectionName, commonpb.ObjectPrivilege_PrivilegeSkipRLS.String())
	if err != nil {
		return err
	}
	if permitted {
		return nil
	}
	return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: skip_rls requires SkipRLS privilege on collection %s", operation, collectionName)
}

// resolveRLSEnforcement returns whether RLS remains enabled after processing a
// request-scoped bypass. rls.force takes precedence over both authorization
// configuration and SkipRLS privileges.
func resolveRLSEnforcement(ctx context.Context, metaCache Cache, rlsEnabled, rlsForce, skipRLS bool, dbName, collectionName, operation string) (bool, error) {
	if !rlsEnabled || !skipRLS {
		return rlsEnabled, nil
	}
	if rlsForce {
		return false, merr.WrapErrPrivilegeNotPermitted(
			"%s operation denied by RLS: skip_rls is not allowed when rls.force is enabled on collection %s",
			operation, collectionName)
	}
	if err := checkSkipRLSPrivilege(ctx, metaCache, dbName, collectionName, operation); err != nil {
		return false, err
	}
	return false, nil
}

func isCurrentUserPermitted(ctx context.Context, metaCache Cache, dbName, objectType, objectName, objectPrivilege string) (bool, error) {
	if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		return true, nil
	}
	username, _, err := contextutil.GetAuthInfoFromContext(ctx)
	if err != nil {
		mlog.Warn(ctx, "GetCurUserFromContext fail", mlog.Err(err))
		return false, err
	}
	if !Params.CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		return true, nil
	}
	if dbName == "" {
		dbName = GetCurDBNameFromContextOrDefault(ctx)
	}
	roleNames, err := GetRole(username)
	if err != nil {
		mlog.Warn(ctx, "GetRole fail", mlog.String("username", username), mlog.Err(err))
		return false, err
	}
	roleNames = append(roleNames, util.RolePublic)

	if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() && objectType == commonpb.ObjectType_Collection.String() {
		if objectName != util.AnyWord && objectName != "" {
			if actualCollectionName, resolveErr := resolveCollectionAlias(ctx, metaCache, dbName, objectName); resolveErr != nil {
				mlog.RatedWarn(ctx, rate.Limit(60), "failed to resolve collection alias for RBAC, using original name",
					mlog.String("objectName", objectName), mlog.FieldDbName(dbName), mlog.Err(resolveErr))
			} else {
				objectName = actualCollectionName
			}
		}
	}

	e := privilege.GetEnforcer()
	for _, roleName := range roleNames {
		object := funcutil.PolicyForResource(dbName, objectType, objectName)
		isPermit, cached, version := privilege.GetResultCache(roleName, object, objectPrivilege)
		if cached {
			if isPermit {
				return true, nil
			}
			continue
		}
		isPermit, err := e.Enforce(roleName, object, objectPrivilege)
		if err != nil {
			return false, err
		}
		privilege.SetResultCache(roleName, object, objectPrivilege, isPermit, version)
		if isPermit {
			return true, nil
		}
	}
	return false, nil
}

// resolveCollectionAlias resolves an alias to its actual collection name
func resolveCollectionAlias(ctx context.Context, metaCache Cache, dbName, nameOrAlias string) (string, error) {
	if metaCache == nil {
		return nameOrAlias, merr.WrapErrServiceInternal("meta cache not initialized")
	}
	return metaCache.ResolveCollectionAlias(ctx, dbName, nameOrAlias)
}
