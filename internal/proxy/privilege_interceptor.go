package proxy

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
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

// streamPrivilegeExt describes the RBAC requirement of a streaming RPC. Streaming
// interceptors have no `req` object to reflect the privilege from (unlike the unary
// PrivilegeInterceptor which resolves it via funcutil.GetPrivilegeExtObj(req)), so the
// required privilege is declared statically per full-method name.
type streamPrivilegeExt struct {
	objectType      string
	objectPrivilege string
}

// streamMethodPrivileges is the static resource-privilege table for streaming RPCs.
// It maps a streaming RPC's full method name to the cluster-level privilege it requires.
// Both streaming methods on MilvusService touch the WAL data plane (CreateReplicateStream
// writes replicate messages; DumpMessages streams messages out for data salvage), so both
// require the cluster-admin-level replicate privilege. Any streaming method NOT present in
// this table is denied by default (fail-closed) in StreamPrivilegeInterceptor, which
// prevents a newly-added stream RPC from silently bypassing the RBAC chain.
var streamMethodPrivileges = map[string]streamPrivilegeExt{
	milvuspb.MilvusService_CreateReplicateStream_FullMethodName: {
		objectType:      commonpb.ObjectType_Global.String(),
		objectPrivilege: commonpb.ObjectPrivilege_PrivilegeUpdateReplicateConfiguration.String(),
	},
	milvuspb.MilvusService_DumpMessages_FullMethodName: {
		objectType:      commonpb.ObjectType_Global.String(),
		objectPrivilege: commonpb.ObjectPrivilege_PrivilegeUpdateReplicateConfiguration.String(),
	},
}

// StreamPrivilegeFunc is the streaming counterpart of PrivilegeFunc. It authorizes a
// streaming call using only the context (which already carries the authenticated user)
// and the full method name (which identifies the required privilege via the static
// streamMethodPrivileges table), returning the (possibly enriched) context on success.
type StreamPrivilegeFunc func(ctx context.Context, fullMethod string) (context.Context, error)

// PrivilegeStreamInterceptor returns a new stream server interceptor that performs
// per-stream privilege access. It mirrors UnaryServerInterceptor: it takes the
// authorization function as a parameter and wraps it into a grpc.StreamServerInterceptor,
// propagating the authorized context to the handler via a wrapped ServerStream.
func PrivilegeStreamInterceptor(privilegeFunc StreamPrivilegeFunc) grpc.StreamServerInterceptor {
	privilege.InitPrivilegeGroups()
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := privilegeFunc(ss.Context(), info.FullMethod)
		if err != nil {
			hookutil.GetExtension().ReportAction(newCtx, nil, &milvuspb.BoolResponse{
				Status: merr.Status(err),
			}, err, info.FullMethod, hookutil.ActionAuthorize)
			return err
		}
		wrapped := grpc_middleware.WrapServerStream(ss)
		wrapped.WrappedContext = newCtx
		return handler(srv, wrapped)
	}
}

// StreamPrivilegeInterceptor is the streaming counterpart of PrivilegeInterceptor. It
// performs the same role→casbin authorization, but resolves the required privilege from
// the full method name (via streamMethodPrivileges) instead of the absent request object,
// and treats the operation as cluster/Global level (objectName == util.AnyWord).
func StreamPrivilegeInterceptor(ctx context.Context, fullMethod string) (context.Context, error) {
	if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		return ctx, nil
	}

	ext, ok := streamMethodPrivileges[fullMethod]
	if !ok {
		// Unregistered streaming method: deny by default so no stream RPC can slip
		// through the authorization chain unnoticed.
		mlog.Warn(ctx, "stream method not registered for privilege check, denying", mlog.String("method", fullMethod))
		return ctx, status.Error(codes.PermissionDenied, fmt.Sprintf("streaming method %s is not authorized", fullMethod))
	}

	username, password, err := contextutil.GetAuthInfoFromContext(ctx)
	if err != nil {
		mlog.Warn(ctx, "GetAuthInfoFromContext fail for stream", mlog.Err(err))
		return ctx, err
	}
	if !Params.CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		return ctx, nil
	}
	roleNames, err := GetRole(username)
	if err != nil {
		mlog.Warn(ctx, "GetRole fail for stream", mlog.String("username", username), mlog.Err(err))
		return ctx, err
	}
	roleNames = append(roleNames, util.RolePublic)
	ctx = SetRBACRolesToContext(ctx, roleNames)

	dbName := GetCurDBNameFromContextOrDefault(ctx)
	object := funcutil.PolicyForResource(dbName, ext.objectType, util.AnyWord)
	log := mlog.With(mlog.String("username", username), mlog.Strings("role_names", roleNames),
		mlog.String("object_type", ext.objectType), mlog.String("object_privilege", ext.objectPrivilege),
		mlog.FieldDbName(dbName), mlog.String("method", fullMethod))

	e := privilege.GetEnforcer()
	for _, roleName := range roleNames {
		isPermit, cached, version := privilege.GetResultCache(roleName, object, ext.objectPrivilege)
		if !cached {
			isPermit, err = e.Enforce(roleName, object, ext.objectPrivilege)
			if err != nil {
				log.Warn(ctx, "fail to execute permit func for stream", mlog.Err(err))
				return ctx, err
			}
			privilege.SetResultCache(roleName, object, ext.objectPrivilege, isPermit, version)
		}
		if isPermit {
			return ctx, nil
		}
	}

	log.Info(ctx, "permission deny for stream", mlog.Strings("roles", roleNames))
	if password == util.PasswordHolder {
		username = "apikey user"
	}
	return ctx, status.Error(codes.PermissionDenied,
		fmt.Sprintf("%s: permission deny to %s in the `%s` database", ext.objectPrivilege, username, dbName))
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
