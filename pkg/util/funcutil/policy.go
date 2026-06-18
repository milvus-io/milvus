package funcutil

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/time/rate"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func GetVersion(m interface{}) (string, error) {
	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := merr.WrapErrParameterInvalidMsg("MessageDescriptorProto result is nil")
		mlog.RatedInfo(context.TODO(), rate.Limit(60), "GetVersion failed", mlog.Err(err))
		return "", err
	}
	if !proto.HasExtension(pbMsg.ProtoReflect().Descriptor().Options(), milvuspb.E_MilvusExtObj) {
		err := merr.WrapErrParameterInvalidMsg("Extension not found")
		mlog.Error(context.TODO(), "GetExtension fail", mlog.Err(err))
		return "", err
	}
	extObj := proto.GetExtension(pbMsg.ProtoReflect().Descriptor().Options(), milvuspb.E_MilvusExtObj)
	version := extObj.(*milvuspb.MilvusExt).Version
	mlog.Debug(context.TODO(), "GetVersion success", mlog.String("version", version))
	return version, nil
}

func GetPrivilegeExtObj(m interface{}) (commonpb.PrivilegeExt, error) {
	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := merr.WrapErrParameterInvalidMsg("MessageDescriptorProto result is nil")
		mlog.RatedInfo(context.TODO(), rate.Limit(60), "GetPrivilegeExtObj failed", mlog.Err(err))
		return commonpb.PrivilegeExt{}, err
	}

	if !proto.HasExtension(pbMsg.ProtoReflect().Descriptor().Options(), commonpb.E_PrivilegeExtObj) {
		err := merr.WrapErrParameterInvalidMsg("Extension not found")
		mlog.RatedWarn(context.TODO(), rate.Limit(60), "GetPrivilegeExtObj failed", mlog.Err(err))
		return commonpb.PrivilegeExt{}, err
	}
	extObj := proto.GetExtension(pbMsg.ProtoReflect().Descriptor().Options(), commonpb.E_PrivilegeExtObj)

	privilegeExt := extObj.(*commonpb.PrivilegeExt)
	mlog.RatedDebug(context.TODO(), rate.Limit(60), "GetPrivilegeExtObj success", mlog.String("resource_type", privilegeExt.ObjectType.String()), mlog.String("resource_privilege", privilegeExt.ObjectPrivilege.String()))
	return commonpb.PrivilegeExt{
		ObjectType:       privilegeExt.ObjectType,
		ObjectPrivilege:  privilegeExt.ObjectPrivilege,
		ObjectNameIndex:  privilegeExt.ObjectNameIndex,
		ObjectNameIndexs: privilegeExt.ObjectNameIndexs,
	}, nil
}

// GetObjectName get object name from the grpc message according to the field index. The field is a string.
func GetObjectName(m interface{}, index int32) string {
	if index <= 0 {
		return util.AnyWord
	}

	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := merr.WrapErrParameterInvalidMsg("MessageDescriptorProto result is nil")
		mlog.RatedInfo(context.TODO(), rate.Limit(60), "GetObjectName fail", mlog.Err(err))
		return util.AnyWord
	}

	msgDesc := pbMsg.ProtoReflect().Descriptor()
	value := pbMsg.ProtoReflect().Get(msgDesc.Fields().ByNumber(protoreflect.FieldNumber(index)))
	user, ok := value.Interface().(protoreflect.Message)
	if ok {
		userDesc := user.Descriptor()
		value = user.Get(userDesc.Fields().ByNumber(protoreflect.FieldNumber(1)))
		if value.String() == "" {
			return util.AnyWord
		}
	}
	return value.String()
}

// GetObjectNames get object names from the grpc message according to the field index. The field is an array.
func GetObjectNames(m interface{}, index int32) []string {
	if index <= 0 {
		return []string{}
	}

	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := merr.WrapErrParameterInvalidMsg("MessageDescriptorProto result is nil")
		mlog.RatedInfo(context.TODO(), rate.Limit(60), "GetObjectNames fail", mlog.Err(err))
		return []string{}
	}

	msgDesc := pbMsg.ProtoReflect().Descriptor()
	value := pbMsg.ProtoReflect().Get(msgDesc.Fields().ByNumber(protoreflect.FieldNumber(index)))
	names, ok := value.Interface().(protoreflect.List)
	if !ok {
		return []string{}
	}
	res := make([]string, names.Len())
	for i := 0; i < names.Len(); i++ {
		res[i] = names.Get(i).String()
	}
	return res
}

func PolicyForPrivilege(roleName string, objectType string, objectName string, privilege string, dbName string) string {
	return fmt.Sprintf(`{"PType":"p","V0":"%s","V1":"%s","V2":"%s"}`, roleName, PolicyForResource(dbName, objectType, objectName), privilege)
}

func PolicyForPrivileges(grants []*milvuspb.GrantEntity) string {
	return strings.Join(lo.Map(grants, func(r *milvuspb.GrantEntity, _ int) string {
		return PolicyForPrivilege(r.Role.Name, r.Object.Name, r.ObjectName, r.Grantor.Privilege.Name, r.DbName)
	}), "|")
}

func PrivilegesForPolicy(policy string) []string {
	return strings.Split(policy, "|")
}

func PolicyForResource(dbName string, objectType string, objectName string) string {
	return fmt.Sprintf("%s-%s", objectType, CombineObjectName(dbName, objectName))
}

func CombineObjectName(dbName string, objectName string) string {
	if dbName == "" {
		dbName = util.DefaultDBName
	}
	return fmt.Sprintf("%s.%s", dbName, objectName)
}

func SplitObjectName(objectName string) (string, string) {
	if !strings.Contains(objectName, ".") {
		return util.DefaultDBName, objectName
	}
	names := strings.Split(objectName, ".")
	return names[0], names[1]
}

func PolicyCheckerWithRole(policy, roleName string) bool {
	return strings.Contains(policy, fmt.Sprintf(`"V0":"%s"`, roleName))
}
