package model

import (
	"strings"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util"
)

func ConvertToFieldSchemaPB(field *Field) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:      field.FieldID,
		Name:         field.Name,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  field.Description,
		DataType:     field.DataType,
		TypeParams:   field.TypeParams,
		IndexParams:  field.IndexParams,
		AutoID:       field.AutoID,
	}
}

func BatchConvertToFieldSchemaPB(fields []*Field) []*schemapb.FieldSchema {
	fieldSchemas := make([]*schemapb.FieldSchema, len(fields))
	for idx, field := range fields {
		fieldSchemas[idx] = ConvertToFieldSchemaPB(field)
	}
	return fieldSchemas
}

func ConvertFieldPBToModel(fieldSchema *schemapb.FieldSchema) *Field {
	return &Field{
		FieldID:      fieldSchema.FieldID,
		Name:         fieldSchema.Name,
		IsPrimaryKey: fieldSchema.IsPrimaryKey,
		Description:  fieldSchema.Description,
		DataType:     fieldSchema.DataType,
		TypeParams:   fieldSchema.TypeParams,
		IndexParams:  fieldSchema.IndexParams,
		AutoID:       fieldSchema.AutoID,
	}
}

func BatchConvertFieldPBToModel(fieldSchemas []*schemapb.FieldSchema) []*Field {
	fields := make([]*Field, len(fieldSchemas))
	for idx, fieldSchema := range fieldSchemas {
		fields[idx] = ConvertFieldPBToModel(fieldSchema)
	}
	return fields
}

func ConvertCollectionPBToModel(coll *pb.CollectionInfo, extra map[string]string) *Collection {
	partitions := make([]*Partition, len(coll.PartitionIDs))
	for idx := range coll.PartitionIDs {
		partitions[idx] = &Partition{
			PartitionID:               coll.PartitionIDs[idx],
			PartitionName:             coll.PartitionNames[idx],
			PartitionCreatedTimestamp: coll.PartitionCreatedTimestamps[idx],
		}
	}
	indexes := make([]*Index, len(coll.FieldIndexes))
	for idx, fieldIndexInfo := range coll.FieldIndexes {
		indexes[idx] = &Index{
			FieldID: fieldIndexInfo.FiledID,
			IndexID: fieldIndexInfo.IndexID,
		}
	}
	return &Collection{
		CollectionID:         coll.ID,
		Name:                 coll.Schema.Name,
		Description:          coll.Schema.Description,
		AutoID:               coll.Schema.AutoID,
		Fields:               BatchConvertFieldPBToModel(coll.Schema.Fields),
		Partitions:           partitions,
		FieldIndexes:         indexes,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		StartPositions:       coll.StartPositions,
		Extra:                extra,
	}
}

func CloneCollectionModel(coll Collection) *Collection {
	return &Collection{
		TenantID:             coll.TenantID,
		CollectionID:         coll.CollectionID,
		Name:                 coll.Name,
		Description:          coll.Description,
		AutoID:               coll.AutoID,
		Fields:               coll.Fields,
		Partitions:           coll.Partitions,
		FieldIndexes:         coll.FieldIndexes,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		StartPositions:       coll.StartPositions,
		Aliases:              coll.Aliases,
		Extra:                coll.Extra,
	}
}

func ConvertToCollectionPB(coll *Collection) *pb.CollectionInfo {
	fields := make([]*schemapb.FieldSchema, len(coll.Fields))
	for idx, field := range coll.Fields {
		fields[idx] = &schemapb.FieldSchema{
			FieldID:      field.FieldID,
			Name:         field.Name,
			IsPrimaryKey: field.IsPrimaryKey,
			Description:  field.Description,
			DataType:     field.DataType,
			TypeParams:   field.TypeParams,
			IndexParams:  field.IndexParams,
			AutoID:       field.AutoID,
		}
	}
	collSchema := &schemapb.CollectionSchema{
		Name:        coll.Name,
		Description: coll.Description,
		AutoID:      coll.AutoID,
		Fields:      fields,
	}
	partitionIDs := make([]int64, len(coll.Partitions))
	partitionNames := make([]string, len(coll.Partitions))
	partitionCreatedTimestamps := make([]uint64, len(coll.Partitions))
	for idx, partition := range coll.Partitions {
		partitionIDs[idx] = partition.PartitionID
		partitionNames[idx] = partition.PartitionName
		partitionCreatedTimestamps[idx] = partition.PartitionCreatedTimestamp
	}
	fieldIndexes := make([]*pb.FieldIndexInfo, len(coll.FieldIndexes))
	for idx, index := range coll.FieldIndexes {
		fieldIndexes[idx] = &pb.FieldIndexInfo{
			FiledID: index.FieldID,
			IndexID: index.IndexID,
		}
	}
	return &pb.CollectionInfo{
		ID:                         coll.CollectionID,
		Schema:                     collSchema,
		PartitionIDs:               partitionIDs,
		PartitionNames:             partitionNames,
		FieldIndexes:               fieldIndexes,
		CreateTime:                 coll.CreateTime,
		VirtualChannelNames:        coll.VirtualChannelNames,
		PhysicalChannelNames:       coll.PhysicalChannelNames,
		ShardsNum:                  coll.ShardsNum,
		PartitionCreatedTimestamps: partitionCreatedTimestamps,
		ConsistencyLevel:           coll.ConsistencyLevel,
		StartPositions:             coll.StartPositions,
	}
}

func MergeIndexModel(a *Index, b *Index) *Index {
	if b.SegmentIndexes != nil {
		if a.SegmentIndexes == nil {
			a.SegmentIndexes = b.SegmentIndexes
		} else {
			for segID, segmentIndex := range b.SegmentIndexes {
				a.SegmentIndexes[segID] = segmentIndex
			}
		}
	}

	if a.CollectionID == 0 && b.CollectionID != 0 {
		a.CollectionID = b.CollectionID
	}

	if a.FieldID == 0 && b.FieldID != 0 {
		a.FieldID = b.FieldID
	}

	if a.IndexID == 0 && b.IndexID != 0 {
		a.IndexID = b.IndexID
	}

	if a.IndexName == "" && b.IndexName != "" {
		a.IndexName = b.IndexName
	}

	if a.IndexParams == nil && b.IndexParams != nil {
		a.IndexParams = b.IndexParams
	}

	if a.Extra == nil && b.Extra != nil {
		a.Extra = b.Extra
	}

	return a
}

func ConvertSegmentIndexPBToModel(segIndex *pb.SegmentIndexInfo) *Index {
	return &Index{
		CollectionID: segIndex.CollectionID,
		SegmentIndexes: map[int64]SegmentIndex{
			segIndex.SegmentID: {
				Segment: Segment{
					SegmentID:   segIndex.SegmentID,
					PartitionID: segIndex.PartitionID,
				},
				BuildID:     segIndex.BuildID,
				EnableIndex: segIndex.EnableIndex,
			},
		},
		FieldID: segIndex.FieldID,
		IndexID: segIndex.IndexID,
	}
}

func ConvertIndexPBToModel(indexInfo *pb.IndexInfo) *Index {
	return &Index{
		IndexName:   indexInfo.IndexName,
		IndexID:     indexInfo.IndexID,
		IndexParams: indexInfo.IndexParams,
	}
}

func ConvertToIndexPB(index *Index) *pb.IndexInfo {
	return &pb.IndexInfo{
		IndexName:   index.IndexName,
		IndexID:     index.IndexID,
		IndexParams: index.IndexParams,
	}
}

func ConvertToCredentialPB(cred *Credential) *internalpb.CredentialInfo {
	if cred == nil {
		return nil
	}
	return &internalpb.CredentialInfo{
		Username:          cred.Username,
		EncryptedPassword: cred.EncryptedPassword,
	}
}

func IsEmptyString(str string) bool {
	return str == ""
}

func RemoveInvalidSeparator(str string) string {
	return strings.TrimRight(strings.Replace(str, "//", "/", 1), "/")
}

func IsRevoke(operateType milvuspb.OperatePrivilegeType) bool {
	return operateType == milvuspb.OperatePrivilegeType_Revoke
}

func IsGrant(operateType milvuspb.OperatePrivilegeType) bool {
	return operateType == milvuspb.OperatePrivilegeType_Grant
}

func GetPrincipalName(entity *milvuspb.GrantEntity) string {
	var principalName string
	if entity.Principal == nil {
		return principalName
	}
	if entity.Principal.GetUser() != nil && !IsEmptyString(entity.Principal.GetUser().Name) {
		principalName = entity.Principal.GetUser().Name
	} else if entity.Principal.GetRole() != nil && !IsEmptyString(entity.Principal.GetRole().Name) {
		principalName = entity.Principal.GetRole().Name
	}
	return principalName
}

func GetPrincipalEntity(principalType string, principalName string) *milvuspb.PrincipalEntity {
	var principalEntity *milvuspb.PrincipalEntity
	if principalType == util.UserPrincipalType {
		principalEntity = &milvuspb.PrincipalEntity{
			PrincipalType: principalType,
			Principal: &milvuspb.PrincipalEntity_User{
				User: &milvuspb.UserEntity{Name: principalName},
			},
		}
	} else if principalType == util.RolePrincipalType {
		principalEntity = &milvuspb.PrincipalEntity{
			PrincipalType: principalType,
			Principal: &milvuspb.PrincipalEntity_Role{
				Role: &milvuspb.RoleEntity{Name: principalName},
			},
		}
	}
	return principalEntity
}
