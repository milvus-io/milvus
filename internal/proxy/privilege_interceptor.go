package proxy

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/util"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	jsonadapter "github.com/casbin/json-adapter/v2"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
m = r.sub == p.sub && globMatch(r.obj, p.obj) && globMatch(r.act, p.act) || r.sub == "admin" || r.act == "All"
`
	ModelKey = "casbin"
)

var modelStore = make(map[string]model.Model, 1)

func initPolicyModel() (model.Model, error) {
	if policyModel, ok := modelStore[ModelStr]; ok {
		return policyModel, nil
	}
	policyModel, err := model.NewModelFromString(ModelStr)
	if err != nil {
		log.Error("NewModelFromString fail", zap.String("model", ModelStr), zap.Error(err))
		return nil, err
	}
	modelStore[ModelKey] = policyModel
	return modelStore[ModelKey], nil
}

// UnaryServerInterceptor returns a new unary server interceptors that performs per-request privilege access.
func UnaryServerInterceptor(privilegeFunc PrivilegeFunc) grpc.UnaryServerInterceptor {
	initPolicyModel()
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := privilegeFunc(ctx, req)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

func PrivilegeInterceptor(ctx context.Context, req interface{}) (context.Context, error) {
	if !Params.CommonCfg.AuthorizationEnabled {
		return ctx, nil
	}
	log.Debug("PrivilegeInterceptor", zap.String("type", reflect.TypeOf(req).String()))
	privilegeExt, err := funcutil.GetPrivilegeExtObj(req)
	if err != nil {
		log.Debug("GetPrivilegeExtObj err", zap.Error(err))
		return ctx, nil
	}
	username, err := GetCurUserFromContext(ctx)
	if err != nil {
		log.Error("GetCurUserFromContext fail", zap.Error(err))
		return ctx, err
	}
	if username == util.UserRoot {
		return ctx, nil
	}
	roleNames, err := GetRole(username)
	if err != nil {
		log.Error("GetRole fail", zap.String("username", username), zap.Error(err))
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
	policyInfo := strings.Join(globalMetaCache.GetPrivilegeInfo(ctx), ",")

	log.Debug("current request info", zap.String("username", username), zap.Strings("role_names", roleNames),
		zap.String("object_type", objectType), zap.String("object_privilege", objectPrivilege),
		zap.Int32("object_index", objectNameIndex), zap.String("object_name", objectName),
		zap.Strings("object_names", objectNames),
		zap.String("policy_info", policyInfo))

	policy := fmt.Sprintf("[%s]", policyInfo)
	b := []byte(policy)
	a := jsonadapter.NewAdapter(&b)
	policyModel, err := initPolicyModel()
	if err != nil {
		errStr := "fail to get policy model"
		log.Error(errStr, zap.Error(err))
		return ctx, err
	}
	e, err := casbin.NewEnforcer(policyModel, a)
	if err != nil {
		log.Error("NewEnforcer fail", zap.String("policy", policy), zap.Error(err))
		return ctx, err
	}
	for _, roleName := range roleNames {
		permitFunc := func(resName string) (bool, error) {
			object := funcutil.PolicyForResource(objectType, objectName)
			isPermit, err := e.Enforce(roleName, object, objectPrivilege)
			if err != nil {
				log.Error("Enforce fail", zap.String("role", roleName), zap.String("object", object), zap.String("privilege", objectPrivilege), zap.Error(err))
				return false, err
			}
			return isPermit, nil
		}

		// handle the api which refers one resource
		permitObject, err := permitFunc(objectName)
		if err != nil {
			return ctx, err
		}
		if permitObject {
			return ctx, nil
		}

		// handle the api which refers many resources
		permitObjects := true
		for _, name := range objectNames {
			p, err := permitFunc(name)
			if err != nil {
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

	log.Debug("permission deny", zap.String("policy", policy), zap.Strings("roles", roleNames))
	return ctx, status.Error(codes.PermissionDenied, "permission deny")
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
