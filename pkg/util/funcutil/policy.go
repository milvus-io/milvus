package funcutil

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/descriptor"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func GetVersion(m proto.GeneratedMessage) (string, error) {
	md, _ := descriptor.MessageDescriptorProto(m)
	if md == nil {
		log.Error("MessageDescriptorProto result is nil")
		return "", fmt.Errorf("MessageDescriptorProto result is nil")
	}
	extObj, err := proto.GetExtension(md.Options, milvuspb.E_MilvusExtObj)
	if err != nil {
		log.Error("GetExtension fail", zap.Error(err))
		return "", err
	}
	version := extObj.(*milvuspb.MilvusExt).Version
	log.Debug("GetVersion success", zap.String("version", version))
	return version, nil
}

func GetPrivilegeExtObj(m proto.GeneratedMessage) (commonpb.PrivilegeExt, error) {
	_, md := descriptor.MessageDescriptorProto(m)
	if md == nil {
		log.Warn("MessageDescriptorProto result is nil")
		return commonpb.PrivilegeExt{}, fmt.Errorf("MessageDescriptorProto result is nil")
	}

	extObj, err := proto.GetExtension(md.Options, commonpb.E_PrivilegeExtObj)
	if err != nil {
		log.Warn("GetExtension fail", zap.Error(err))
		return commonpb.PrivilegeExt{}, err
	}
	privilegeExt := extObj.(*commonpb.PrivilegeExt)
	log.Debug("GetPrivilegeExtObj success", zap.String("resource_type", privilegeExt.ObjectType.String()), zap.String("resource_privilege", privilegeExt.ObjectPrivilege.String()))
	return commonpb.PrivilegeExt{
		ObjectType:       privilegeExt.ObjectType,
		ObjectPrivilege:  privilegeExt.ObjectPrivilege,
		ObjectNameIndex:  privilegeExt.ObjectNameIndex,
		ObjectNameIndexs: privilegeExt.ObjectNameIndexs,
	}, nil
}

// GetObjectName get object name from the grpc message according to the field index. The field is a string.
func GetObjectName(m proto.GeneratedMessage, index int32) string {
	if index <= 0 {
		return util.AnyWord
	}
	msg := proto.MessageReflect(proto.MessageV1(m))
	msgDesc := msg.Descriptor()
	value := msg.Get(msgDesc.Fields().ByNumber(protoreflect.FieldNumber(index)))
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
func GetObjectNames(m proto.GeneratedMessage, index int32) []string {
	if index <= 0 {
		return []string{}
	}
	msg := proto.MessageReflect(proto.MessageV1(m))
	msgDesc := msg.Descriptor()
	value := msg.Get(msgDesc.Fields().ByNumber(protoreflect.FieldNumber(index)))
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
