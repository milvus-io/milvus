// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastAlterCollectionForAddStructField broadcasts the put collection message for add struct field.
func (c *Core) broadcastAlterCollectionForAddStructField(ctx context.Context, req *milvuspb.AddCollectionStructFieldRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	if req.GetStructArrayFieldSchema() == nil {
		return merr.WrapErrParameterInvalidMsg("struct array field schema is nil")
	}
	structArrayField := proto.Clone(req.GetStructArrayFieldSchema()).(*schemapb.StructArrayFieldSchema)
	if err := normalizeAndCheckAddedStructField(structArrayField); err != nil {
		return err
	}
	if err := checkStructArrayFieldSchema([]*schemapb.StructArrayFieldSchema{structArrayField}); err != nil {
		return errors.Wrap(err, "failed to check struct array field schema")
	}
	if err := validateStructArrayFieldDataType([]*schemapb.StructArrayFieldSchema{structArrayField}); err != nil {
		return err
	}
	if err := checkAddStructFieldDuplicate(coll, structArrayField); err != nil {
		return err
	}

	schema := coll.ToCollectionSchemaPB()
	fieldIDStart := maxAssignedFieldIDFromSchema(schema) + 1
	structArrayField.FieldID = fieldIDStart
	for i, field := range structArrayField.GetFields() {
		field.FieldID = fieldIDStart + int64(i) + 1
	}

	schema.Version = coll.SchemaVersion + 1
	schema.StructArrayFields = append(schema.StructArrayFields, structArrayField)
	properties := updateMaxFieldIDProperty(coll.Properties, maxAssignedFieldIDFromSchema(schema))
	schema.Properties = properties

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema, message.FieldMaskCollectionProperties},
			},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema:     schema,
				Properties: properties,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

func normalizeAndCheckAddedStructField(structArrayField *schemapb.StructArrayFieldSchema) error {
	if !structArrayField.GetNullable() {
		return merr.WrapErrParameterInvalidMsg("added struct field must be nullable, please check it, struct field name = %s", structArrayField.GetName())
	}
	if err := validateAddedStructFieldName(structArrayField.GetName()); err != nil {
		return err
	}
	if isReservedStructFieldName(structArrayField.GetName()) {
		return merr.WrapErrParameterInvalidMsg("not support to add system field, field name = %s", structArrayField.GetName())
	}

	for _, field := range structArrayField.GetFields() {
		originalName, err := normalizeStructSubFieldName(structArrayField.GetName(), field)
		if err != nil {
			return err
		}
		if err := validateAddedStructFieldName(originalName); err != nil {
			return err
		}
		if isReservedStructFieldName(originalName) {
			return merr.WrapErrParameterInvalidMsg("not support to add system field, field name = %s", originalName)
		}
		if field.GetIsPrimaryKey() {
			return merr.WrapErrParameterInvalidMsg("primary key is not supported for struct field, field name = %s", originalName)
		}
		if field.GetAutoID() {
			return merr.WrapErrParameterInvalidMsg("autoID is not supported for struct field, field name = %s", originalName)
		}
		if field.GetIsPartitionKey() {
			return merr.WrapErrParameterInvalidMsg("partition key is not supported for struct field, field name = %s", originalName)
		}
		if field.GetIsClusteringKey() {
			return merr.WrapErrParameterInvalidMsg("clustering key is not supported for struct field, field name = %s", originalName)
		}
		if field.GetDefaultValue() != nil {
			return merr.WrapErrParameterInvalidMsg("default value is not supported for struct field, field name = %s", originalName)
		}
		if field.GetIsFunctionOutput() {
			return merr.WrapErrParameterInvalidMsg("function output is not supported for struct field, field name = %s", originalName)
		}
		if field.GetExternalField() != "" {
			return merr.WrapErrParameterInvalidMsg("add struct field operation does not support external field mapping, field name = %s", originalName)
		}
		field.Nullable = true
	}
	return nil
}

func normalizeStructSubFieldName(structName string, field *schemapb.FieldSchema) (string, error) {
	fieldName := field.GetName()
	if isStoredStructSubFieldName(structName, fieldName) {
		originalName, err := typeutil.ExtractStructFieldName(fieldName)
		if err != nil {
			return "", err
		}
		return originalName, nil
	}
	if typeutil.IsStructSubField(fieldName) {
		return "", merr.WrapErrParameterInvalidMsg("invalid struct sub-field name %s for struct field %s", fieldName, structName)
	}

	field.Name = typeutil.ConcatStructFieldName(structName, fieldName)
	return fieldName, nil
}

func checkAddStructFieldDuplicate(coll *model.Collection, structArrayField *schemapb.StructArrayFieldSchema) error {
	fieldNames := make(map[string]struct{})
	addFieldName := func(name string) error {
		if _, ok := fieldNames[name]; ok {
			return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", name)
		}
		fieldNames[name] = struct{}{}
		return nil
	}

	for _, field := range coll.Fields {
		if err := addFieldName(field.Name); err != nil {
			return err
		}
	}
	for _, structField := range coll.StructArrayFields {
		if err := addFieldName(structField.Name); err != nil {
			return err
		}
		for _, field := range structField.Fields {
			if err := addFieldName(field.Name); err != nil {
				return err
			}
			storedName := storedRootStructSubFieldName(structField.Name, field.Name)
			if storedName != field.Name {
				if err := addFieldName(storedName); err != nil {
					return err
				}
			}
		}
	}

	if err := addFieldName(structArrayField.GetName()); err != nil {
		return err
	}
	for _, field := range structArrayField.GetFields() {
		if err := addFieldName(field.GetName()); err != nil {
			return err
		}
	}
	return nil
}

func isStoredStructSubFieldName(structName string, fieldName string) bool {
	return strings.HasPrefix(fieldName, structName+"[") && strings.HasSuffix(fieldName, "]")
}

func storedRootStructSubFieldName(structName string, fieldName string) string {
	if isStoredStructSubFieldName(structName, fieldName) {
		return fieldName
	}
	return typeutil.ConcatStructFieldName(structName, fieldName)
}

func isReservedStructFieldName(fieldName string) bool {
	return funcutil.SliceContain([]string{common.RowIDFieldName, common.TimeStampFieldName, common.MetaFieldName, common.NamespaceFieldName, common.VirtualPKFieldName}, fieldName)
}

func validateAddedStructFieldName(fieldName string) error {
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return merr.WrapErrFieldNameInvalid(fieldName, "field name should not be empty")
	}

	invalidMsg := "Invalid field name: " + fieldName + ". "
	if len(fieldName) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a field name must be less than " + Params.ProxyCfg.MaxNameLength.GetValue() + " characters."
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}

	firstChar := fieldName[0]
	if firstChar != '_' && !isAlphaForStructFieldName(firstChar) {
		msg := invalidMsg + "The first character of a field name must be an underscore or letter."
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}

	for i := 1; i < len(fieldName); i++ {
		c := fieldName[i]
		if c != '_' && !isAlphaForStructFieldName(c) && !isNumberForStructFieldName(c) {
			msg := invalidMsg + "Field name can only contain numbers, letters, and underscores."
			return merr.WrapErrFieldNameInvalid(fieldName, msg)
		}
	}
	if common.IsFieldNameKeyword(fieldName) {
		msg := invalidMsg + fmt.Sprintf("%s is keyword in milvus.", fieldName)
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}
	return nil
}

func isAlphaForStructFieldName(c uint8) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isNumberForStructFieldName(c uint8) bool {
	return c >= '0' && c <= '9'
}
