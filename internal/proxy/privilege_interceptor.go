package proxy

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	jsonadapter "github.com/casbin/json-adapter/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
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
m = r.sub == p.sub && globMatch(r.obj, p.obj) && globMatch(r.act, p.act) || r.sub == "admin" || (r.sub == p.sub && dbMatch(r.obj, p.obj) && p.act == "PrivilegeAll")
`
)

var templateModel = getPolicyModel(ModelStr)

func getPolicyModel(modelString string) model.Model {
	m, err := model.NewModelFromString(modelString)
	if err != nil {
		log.Panic("NewModelFromString fail", zap.String("model", ModelStr), zap.Error(err))
	}
	return m
}

// UnaryServerInterceptor returns a new unary server interceptors that performs per-request privilege access.
func UnaryServerInterceptor(privilegeFunc PrivilegeFunc) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
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
	log.Debug("PrivilegeInterceptor", zap.String("type", reflect.TypeOf(req).String()))
	privilegeExt, err := funcutil.GetPrivilegeExtObj(req)
	if err != nil {
		log.Warn("GetPrivilegeExtObj err", zap.Error(err))
		return ctx, nil
	}
	username, err := GetCurUserFromContext(ctx)
	if err != nil {
		log.Warn("GetCurUserFromContext fail", zap.Error(err))
		return ctx, err
	}
	if username == util.UserRoot {
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
	objectNameIndexs := privilegeExt.ObjectNameIndexs
	objectNames := funcutil.GetObjectNames(req, objectNameIndexs)
	objectPrivilege := privilegeExt.ObjectPrivilege.String()
	dbName := GetCurDBNameFromContextOrDefault(ctx)
	policyInfo := strings.Join(globalMetaCache.GetPrivilegeInfo(ctx), ",")

	log := log.With(zap.String("username", username), zap.Strings("role_names", roleNames),
		zap.String("object_type", objectType), zap.String("object_privilege", objectPrivilege),
		zap.String("db_name", dbName),
		zap.Int32("object_index", objectNameIndex), zap.String("object_name", objectName),
		zap.Int32("object_indexs", objectNameIndexs), zap.Strings("object_names", objectNames),
		zap.String("policy_info", policyInfo))

	policy := fmt.Sprintf("[%s]", policyInfo)
	b := []byte(policy)
	a := jsonadapter.NewAdapter(&b)
	// the `templateModel` object isn't safe in the concurrent situation
	casbinModel := templateModel.Copy()
	e, err := casbin.NewEnforcer(casbinModel, a)
	if err != nil {
		log.Warn("NewEnforcer fail", zap.String("policy", policy), zap.Error(err))
		return ctx, err
	}
	e.AddFunction("dbMatch", DBMatchFunc)
	for _, roleName := range roleNames {
		permitFunc := func(resName string) (bool, error) {
			object := funcutil.PolicyForResource(dbName, objectType, resName)
			isPermit, err := e.Enforce(roleName, object, objectPrivilege)
			if err != nil {
				return false, err
			}
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

	log.Info("permission deny", zap.String("policy", policy), zap.Strings("roles", roleNames))
	return ctx, status.Error(codes.PermissionDenied, fmt.Sprintf("%s: permission deny", objectPrivilege))
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

func DBMatchFunc(args ...interface{}) (interface{}, error) {
	name1 := args[0].(string)
	name2 := args[1].(string)

	db1, _ := funcutil.SplitObjectName(name1[strings.Index(name1, "-")+1:])
	db2, _ := funcutil.SplitObjectName(name2[strings.Index(name2, "-")+1:])

	return db1 == db2, nil
}
