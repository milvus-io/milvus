package proxy

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type PrivilegeFunc func(ctx context.Context, req interface{}) (context.Context, error)

const (
	// sub -> role name, like admin, public
	// obj -> contact object with object name, like Global-*, Collection-col1
	// act -> privilege, like CreateCollection, DescribeCollection
	ModelStr = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && globMatch(r.obj, p.obj) && globMatch(r.act, p.act) || r.sub == "admin" || (r.sub == p.sub && dbMatch(r.obj, p.obj) && privilegeGroupContains(r.act, p.act, r.obj, p.obj))
`
)

var templateModel = getPolicyModel(ModelStr)

var (
	enforcer                *casbin.SyncedEnforcer
	initOnce                sync.Once
	initPrivilegeGroupsOnce sync.Once
)

var roPrivileges, rwPrivileges, adminPrivileges map[string]struct{}

func initPrivilegeGroups() {
	initPrivilegeGroupsOnce.Do(func() {
		roGroup := paramtable.Get().CommonCfg.ReadOnlyPrivileges.GetAsStrings()
		if len(roGroup) == 0 {
			roGroup = util.ReadOnlyPrivilegeGroup
		}
		roPrivileges = lo.SliceToMap(roGroup, func(item string) (string, struct{}) { return item, struct{}{} })

		rwGroup := paramtable.Get().CommonCfg.ReadWritePrivileges.GetAsStrings()
		if len(rwGroup) == 0 {
			rwGroup = util.ReadWritePrivilegeGroup
		}
		rwPrivileges = lo.SliceToMap(rwGroup, func(item string) (string, struct{}) { return item, struct{}{} })

		adminGroup := paramtable.Get().CommonCfg.AdminPrivileges.GetAsStrings()
		if len(adminGroup) == 0 {
			adminGroup = util.AdminPrivilegeGroup
		}
		adminPrivileges = lo.SliceToMap(adminGroup, func(item string) (string, struct{}) { return item, struct{}{} })
	})
}

func getEnforcer() *casbin.SyncedEnforcer {
	initOnce.Do(func() {
		e, err := casbin.NewSyncedEnforcer()
		if err != nil {
			log.Panic("failed to create casbin enforcer", zap.Error(err))
		}
		casbinModel := getPolicyModel(ModelStr)
		adapter := NewMetaCacheCasbinAdapter(func() Cache { return globalMetaCache })
		e.InitWithModelAndAdapter(casbinModel, adapter)
		e.AddFunction("dbMatch", DBMatchFunc)
		e.AddFunction("privilegeGroupContains", PrivilegeGroupContains)
		enforcer = e
	})
	return enforcer
}

func getPolicyModel(modelString string) model.Model {
	m, err := model.NewModelFromString(modelString)
	if err != nil {
		log.Panic("NewModelFromString fail", zap.String("model", ModelStr), zap.Error(err))
	}
	return m
}

// UnaryServerInterceptor returns a new unary server interceptors that performs per-request privilege access.
func UnaryServerInterceptor(privilegeFunc PrivilegeFunc) grpc.UnaryServerInterceptor {
	initPrivilegeGroups()
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
	log := log.Ctx(ctx)
	log.RatedDebug(60, "PrivilegeInterceptor", zap.String("type", reflect.TypeOf(req).String()))
	privilegeExt, err := funcutil.GetPrivilegeExtObj(req)
	if err != nil {
		log.RatedInfo(60, "GetPrivilegeExtObj err", zap.Error(err))
		return ctx, nil
	}
	username, password, err := contextutil.GetAuthInfoFromContext(ctx)
	if err != nil {
		log.Warn("GetCurUserFromContext fail", zap.Error(err))
		return ctx, err
	}
	if !Params.CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		return ctx, nil
	}
	roleNames, err := GetRole(username)
	if err != nil {
		log.Warn("GetRole fail", zap.String("username", username), zap.Error(err))
		return ctx, err
	}
	roleNames = append(roleNames, util.RolePublic)
	objectType := privilegeExt.ObjectType.String()
	objectNameIndex := privilegeExt.ObjectNameIndex
	objectName := funcutil.GetObjectName(req, objectNameIndex)
	if isCurUserObject(objectType, username, objectName) {
		return ctx, nil
	}

	if isSelectMyRoleGrants(req, roleNames) {
		return ctx, nil
	}

	objectNameIndexs := privilegeExt.ObjectNameIndexs
	objectNames := funcutil.GetObjectNames(req, objectNameIndexs)
	objectPrivilege := privilegeExt.ObjectPrivilege.String()
	dbName := GetCurDBNameFromContextOrDefault(ctx)

	log = log.With(zap.String("username", username), zap.Strings("role_names", roleNames),
		zap.String("object_type", objectType), zap.String("object_privilege", objectPrivilege),
		zap.String("db_name", dbName),
		zap.Int32("object_index", objectNameIndex), zap.String("object_name", objectName),
		zap.Int32("object_indexs", objectNameIndexs), zap.Strings("object_names", objectNames))

	e := getEnforcer()
	for _, roleName := range roleNames {
		permitFunc := func(objectName string) (bool, error) {
			object := funcutil.PolicyForResource(dbName, objectType, objectName)
			isPermit, cached, version := GetPrivilegeCache(roleName, object, objectPrivilege)
			if cached {
				return isPermit, nil
			}
			isPermit, err := e.Enforce(roleName, object, objectPrivilege)
			if err != nil {
				return false, err
			}
			SetPrivilegeCache(roleName, object, objectPrivilege, isPermit, version)
			return isPermit, nil
		}

		if objectNameIndex != 0 {
			// handle the api which refers one resource
			permitObject, err := permitFunc(objectName)
			if err != nil {
				log.Warn("fail to execute permit func", zap.String("name", objectName), zap.Error(err))
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
					log.Warn("fail to execute permit func", zap.String("name", name), zap.Error(err))
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

	log.Info("permission deny", zap.Strings("roles", roleNames))

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

func DBMatchFunc(args ...interface{}) (interface{}, error) {
	name1 := args[0].(string)
	name2 := args[1].(string)

	db1, _ := funcutil.SplitObjectName(name1[strings.Index(name1, "-")+1:])
	db2, _ := funcutil.SplitObjectName(name2[strings.Index(name2, "-")+1:])

	return db1 == db2, nil
}

func collMatch(requestObj, policyObj string) bool {
	_, coll1 := funcutil.SplitObjectName(requestObj[strings.Index(requestObj, "-")+1:])
	_, coll2 := funcutil.SplitObjectName(policyObj[strings.Index(policyObj, "-")+1:])

	return coll1 == util.AnyWord || coll2 == util.AnyWord || coll1 == coll2
}

func PrivilegeGroupContains(args ...interface{}) (interface{}, error) {
	requestPrivilege := args[0].(string)
	policyPrivilege := args[1].(string)
	requestObj := args[2].(string)
	policyObj := args[3].(string)

	switch policyPrivilege {
	case commonpb.ObjectPrivilege_PrivilegeAll.String():
		return true, nil
	case commonpb.ObjectPrivilege_PrivilegeGroupReadOnly.String():
		// read only belong to collection object
		if !collMatch(requestObj, policyObj) {
			return false, nil
		}
		_, ok := roPrivileges[requestPrivilege]
		return ok, nil
	case commonpb.ObjectPrivilege_PrivilegeGroupReadWrite.String():
		// read write belong to collection object
		if !collMatch(requestObj, policyObj) {
			return false, nil
		}
		_, ok := rwPrivileges[requestPrivilege]
		return ok, nil
	case commonpb.ObjectPrivilege_PrivilegeGroupAdmin.String():
		// admin belong to global object
		_, ok := adminPrivileges[requestPrivilege]
		return ok, nil
	default:
		return false, nil
	}
}
