package privilege

import (
	"log"
	"strings"
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

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

var (
	enforcer                *casbin.SyncedEnforcer
	initOnce                sync.Once
	initPrivilegeGroupsOnce sync.Once
)

func GetPolicyModel(modelString string) model.Model {
	m, err := model.NewModelFromString(modelString)
	if err != nil {
		log.Panic("NewModelFromString fail", zap.String("model", ModelStr), zap.Error(err))
	}
	return m
}

func InitPrivilegeGroups() {
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

func GetEnforcer() *casbin.SyncedEnforcer {
	initOnce.Do(func() {
		e, err := casbin.NewSyncedEnforcer()
		if err != nil {
			log.Panic("failed to create casbin enforcer", zap.Error(err))
		}
		casbinModel := GetPolicyModel(ModelStr)
		adapter := NewMetaCacheCasbinAdapter(func() PrivilegeCache { return GetPrivilegeCache() })
		e.InitWithModelAndAdapter(casbinModel, adapter)
		e.AddFunction("dbMatch", DBMatchFunc)
		e.AddFunction("privilegeGroupContains", PrivilegeGroupContains)
		enforcer = e
	})
	return enforcer
}

var roPrivileges, rwPrivileges, adminPrivileges map[string]struct{}

func DBMatchFunc(args ...interface{}) (interface{}, error) {
	name1 := args[0].(string)
	name2 := args[1].(string)

	db1, _ := funcutil.SplitObjectName(name1[strings.Index(name1, "-")+1:])
	db2, _ := funcutil.SplitObjectName(name2[strings.Index(name2, "-")+1:])

	return db1 == db2, nil
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

func collMatch(requestObj, policyObj string) bool {
	_, coll1 := funcutil.SplitObjectName(requestObj[strings.Index(requestObj, "-")+1:])
	_, coll2 := funcutil.SplitObjectName(policyObj[strings.Index(policyObj, "-")+1:])

	return coll1 == util.AnyWord || coll2 == util.AnyWord || coll1 == coll2
}
