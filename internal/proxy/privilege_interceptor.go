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
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type PrivilegeFunc func(ctx context.Context, req interface{}) (context.Context, error)

var (
	initOnce                sync.Once
	initPrivilegeGroupsOnce sync.Once
)

var roPrivileges, rwPrivileges, adminPrivileges map[string]struct{}

// UnaryServerInterceptor returns a new unary server interceptors that performs per-request privilege access.
func UnaryServerInterceptor(privilegeFunc PrivilegeFunc) grpc.UnaryServerInterceptor {
	privilege.InitPrivilegeGroups()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := privilegeFunc(ctx, req)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

func PrivilegeInterceptor(ctx context.Context, req interface{}) (context.Context, error) {
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
	objectType := privilegeExt.ObjectType.String()
	objectNameIndex := privilegeExt.ObjectNameIndex
	objectName := funcutil.GetObjectName(req, objectNameIndex)
	objectPrivilege := privilegeExt.ObjectPrivilege.String()
	// Authorize against the db the request actually operates on, resolved by
	// privilege level (mirrors the grant-side validation; see
	// milvus-io/milvus#50678):
	//   - Cluster-level privileges (CreateDatabase/ResourceGroup/...) are not
	//     scoped to a database, so authorize them globally (AnyWord),
	//     independent of the connection namespace.
	//   - Database-/Collection-level privileges are scoped to the db the request
	//     targets: the request-body DbName takes precedence, falling back to the
	//     connection-context db.
	dbName := GetCurDBNameFromRequestOrContext(ctx, req)
	if util.GetPrivilegeLevel(util.MetaStore2API(objectPrivilege)) == milvuspb.PrivilegeLevel_Cluster.String() {
		dbName = util.AnyWord
	}
	// RenameCollection is a database-admin privilege: a same-db rename is
	// authorized against the target db (database level, handled above), while a
	// cross-db rename additionally requires a cluster-scoped (global) grant.
	if r, ok := req.(*milvuspb.RenameCollectionRequest); ok && r.GetDbName() != r.GetNewDBName() {
		dbName = util.AnyWord
	}

	// Resolve alias to actual collection name for RBAC checks
	if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() && objectType == commonpb.ObjectType_Collection.String() && objectNameIndex != 0 {
		if objectName != util.AnyWord && objectName != "" {
			if actualCollectionName, resolveErr := resolveCollectionAlias(ctx, dbName, objectName); resolveErr != nil {
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
			if actualName, resolveErr := resolveCollectionAlias(ctx, dbName, name); resolveErr != nil {
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
		mlog.FieldDbName(dbName),
		mlog.Int32("object_index", objectNameIndex), mlog.String("object_name", objectName),
		mlog.Int32("object_indexs", objectNameIndexs), mlog.Strings("object_names", objectNames))

	e := privilege.GetEnforcer()
	for _, roleName := range roleNames {
		permitFunc := func(objectName string) (bool, error) {
			object := funcutil.PolicyForResource(dbName, objectType, objectName)
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
		fmt.Sprintf("%s: permission deny to %s in the `%s` database", objectPrivilege, username, dbName))
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

// resolveCollectionAlias resolves an alias to its actual collection name
func resolveCollectionAlias(ctx context.Context, dbName, nameOrAlias string) (string, error) {
	cache := globalMetaCache
	if cache == nil {
		return nameOrAlias, merr.WrapErrServiceInternal("meta cache not initialized")
	}
	return cache.ResolveCollectionAlias(ctx, dbName, nameOrAlias)
}
