package funcutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func GetVersion(m interface{}) (string, error) {
	log := log.Ctx(context.TODO())
	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetVersion failed", zap.Error(err))
		return "", err
	}
	if !proto.HasExtension(pbMsg.ProtoReflect().Descriptor().Options(), milvuspb.E_MilvusExtObj) {
		err := errors.New("Extension not found")
		log.Error("GetExtension fail", zap.Error(err))
		return "", err
	}
	extObj := proto.GetExtension(pbMsg.ProtoReflect().Descriptor().Options(), milvuspb.E_MilvusExtObj)
	version := extObj.(*milvuspb.MilvusExt).Version
	log.Debug("GetVersion success", zap.String("version", version))
	return version, nil
}

func GetPrivilegeExtObj(m interface{}) (commonpb.PrivilegeExt, error) {
	pbMsg, ok := m.(proto.Message)
	if !ok {
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetPrivilegeExtObj failed", zap.Error(err))
		return commonpb.PrivilegeExt{}, err
	}

	if !proto.HasExtension(pbMsg.ProtoReflect().Descriptor().Options(), commonpb.E_PrivilegeExtObj) {
		err := errors.New("Extension not found")
		log.RatedWarn(60, "GetPrivilegeExtObj failed", zap.Error(err))
		return commonpb.PrivilegeExt{}, err
	}
	extObj := proto.GetExtension(pbMsg.ProtoReflect().Descriptor().Options(), commonpb.E_PrivilegeExtObj)

	privilegeExt := extObj.(*commonpb.PrivilegeExt)
	log.RatedDebug(60, "GetPrivilegeExtObj success", zap.String("resource_type", privilegeExt.ObjectType.String()), zap.String("resource_privilege", privilegeExt.ObjectPrivilege.String()))
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
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetObjectName fail", zap.Error(err))
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
		err := errors.New("MessageDescriptorProto result is nil")
		log.RatedInfo(60, "GetObjectNames fail", zap.Error(err))
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

// PolicyForPrivilege builds a Casbin policy string for one privilege grant.
// dbName is the database identifier — it may be a human-readable name (e.g. "default")
// or an ID-based key (e.g. "dbID:1"). The caller is responsible for formatting.
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

// PolicyForResource builds the resource part of a Casbin policy string.
// dbPart is the database identifier — it may be a human-readable name (e.g. "default")
// or an ID-based key (e.g. "dbID:1"). The caller is responsible for formatting.
func PolicyForResource(dbPart string, objectType string, objectName string) string {
	return fmt.Sprintf("%s-%s", objectType, CombineObjectName(dbPart, objectName))
}

// dbPart can be a name ("default") or an ID-based string ("dbID:1").
func CombineObjectName(dbPart string, objectName string) string {
	if dbPart == "" {
		dbPart = util.DefaultDBName
	}
	return fmt.Sprintf("%s.%s", dbPart, objectName)
}

func SplitObjectName(objectName string) (string, string) {
	if !strings.Contains(objectName, ".") {
		return util.DefaultDBName, objectName
	}
	names := strings.SplitN(objectName, ".", 2)
	return names[0], names[1]
}

func PolicyCheckerWithRole(policy, roleName string) bool {
	return strings.Contains(policy, fmt.Sprintf(`"V0":"%s"`, roleName))
}

const collectionIDPrefix = "colID:"

// FormatCollectionID returns the ID-based object name format "colID:12345".
func FormatCollectionID(collectionID int64) string {
	return fmt.Sprintf("%s%d", collectionIDPrefix, collectionID)
}

// IsIDBasedObjectName checks whether objectName uses the ID-based format "colID:xxx".
func IsIDBasedObjectName(objectName string) bool {
	return strings.HasPrefix(objectName, collectionIDPrefix)
}

// ExtractCollectionID parses the collection ID from an ID-based object name "colID:12345".
func ExtractCollectionID(objectName string) (int64, error) {
	if !IsIDBasedObjectName(objectName) {
		return 0, errors.Newf("objectName %q is not ID-based", objectName)
	}
	return strconv.ParseInt(objectName[len(collectionIDPrefix):], 10, 64)
}

const databaseIDPrefix = "dbID:"

// FormatDatabaseID returns the ID-based database name format "dbID:123".
func FormatDatabaseID(dbID int64) string {
	return fmt.Sprintf("%s%d", databaseIDPrefix, dbID)
}

// IsIDBasedDBName checks whether dbName uses the ID-based format "dbID:xxx".
func IsIDBasedDBName(dbName string) bool {
	return strings.HasPrefix(dbName, databaseIDPrefix)
}

// ExtractDatabaseID parses the database ID from an ID-based database name "dbID:123".
func ExtractDatabaseID(dbName string) (int64, error) {
	if !IsIDBasedDBName(dbName) {
		return 0, errors.Newf("dbName %q is not ID-based", dbName)
	}
	return strconv.ParseInt(dbName[len(databaseIDPrefix):], 10, 64)
}
