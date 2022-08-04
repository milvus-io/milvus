package proxy

import (
	"context"
	"fmt"
	"reflect"
	"strings"

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
m = r.sub == p.sub && globMatch(p.obj, r.obj) && globMatch(p.act, r.act) || r.sub == "admin" || r.act == "All"
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
	roleNames, err := GetRole(username)
	if err != nil {
		log.Error("GetRole fail", zap.String("username", username), zap.Error(err))
		return ctx, err
	}
	roleNames = append(roleNames, util.RolePublic)
	resourceType := privilegeExt.ObjectType.String()
	resourceNameIndex := privilegeExt.ObjectNameIndex
	resourceName := funcutil.GetResourceName(req, privilegeExt.ObjectNameIndex)
	resourcePrivilege := privilegeExt.ObjectPrivilege.String()
	policyInfo := strings.Join(globalMetaCache.GetPrivilegeInfo(ctx), ",")

	log.Debug("current request info", zap.String("username", username), zap.Strings("role_names", roleNames),
		zap.String("resource_type", resourceType), zap.String("resource_privilege", resourcePrivilege), zap.Int32("resource_index", resourceNameIndex), zap.String("resource_name", resourceName),
		zap.String("policy_info", policyInfo))

	policy := fmt.Sprintf("[%s]", strings.Join(globalMetaCache.GetPrivilegeInfo(ctx), ","))
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
		isPermit, err := e.Enforce(roleName, funcutil.PolicyForResource(resourceType, resourceName), privilegeExt.ObjectPrivilege.String())
		if err != nil {
			log.Error("Enforce fail", zap.String("policy", policy), zap.String("role", roleName), zap.Error(err))
			return ctx, err
		}
		if isPermit {
			return ctx, nil
		}
	}

	log.Debug("permission deny", zap.String("policy", policy), zap.Strings("roles", roleNames))
	return ctx, &ErrPermissionDenied{}
}

type ErrPermissionDenied struct{}

func (e ErrPermissionDenied) Error() string {
	return "permission deny"
}
