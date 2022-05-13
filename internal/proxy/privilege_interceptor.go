package proxy

import (
	"context"
	"fmt"
	"strings"

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
	ModelStr = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && keyMatch(r.obj, p.obj) && (r.act == p.act || p.act == "ALL" ) || r.sub == "root" || (g(r.sub, "admin") && r.sub != "admin")
`
	ModelKey = "casbin"
)

var modelStore = make(map[string]model.Model, 1)

func initPolicyModel() (model.Model, error) {
	policyModel, err := model.NewModelFromString(ModelStr)
	if err != nil {
		log.Error("NewModelFromString fail", zap.Error(err))
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
	if Params.CommonCfg.AuthorizationEnabled {
		privilegeExt, err := funcutil.GetPrivilegeExtObj(req)
		if err == nil {
			username, err := GetCurUserFromContext(ctx)
			if err != nil {
				log.Error("GetCurUserFromContext fail", zap.Error(err))
				return ctx, err
			}
			resourceType := privilegeExt.ResourceType.String()
			resourceNameIndex := privilegeExt.ResourceNameIndex
			resourceName := funcutil.GetResourceName(req, privilegeExt.ResourceNameIndex)
			resourcePrivilege := privilegeExt.ResourcePrivilege.String()
			policyInfo := strings.Join(globalMetaCache.GetPolicyInfo(ctx), ",")

			log.Debug("username info", zap.String("username", username))
			log.Debug("privilege ext info", zap.String("resource_type", resourceType),
				zap.String("resource_privilege", resourcePrivilege),
				zap.Int32("resource_index", resourceNameIndex),
				zap.String("resource_name", resourceName))
			log.Debug("all policy info", zap.String("policy_info", policyInfo))

			policy := fmt.Sprintf("[%s]", strings.Join(globalMetaCache.GetPolicyInfo(ctx), ","))
			b := []byte(policy)
			a := jsonadapter.NewAdapter(&b)
			policyModel, ok := modelStore[ModelStr]
			if !ok {
				policyModel, err = initPolicyModel()
				if err != nil {
					errStr := "fail to get policy model"
					log.Error(errStr, zap.Error(err))
					return ctx, err
				}
			}
			e, err := casbin.NewEnforcer(policyModel, a)
			if err != nil {
				log.Error("NewEnforcer fail", zap.Error(err))
				return ctx, err
			}
			isPermit, err := e.Enforce(username, funcutil.PolicyForResource(resourceType, resourceName), privilegeExt.ResourcePrivilege.String())
			if err != nil {
				log.Error("Enforce fail", zap.Error(err))
				return ctx, err
			}
			if !isPermit {
				log.Debug("permission deny")
				return ctx, fmt.Errorf("permission deny")
			}
		} else {
			log.Error("GetPrivilegeExtObj err", zap.Error(err))
		}
	}
	return ctx, nil
}
