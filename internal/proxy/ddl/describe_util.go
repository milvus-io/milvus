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

package ddl

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func DescribeCollectionErrorStatus(err error, database, collectionName string) *commonpb.Status {
	// A user-facing DescribeCollection miss is an input error, but keep the
	// precise source code: a missing database is ErrDatabaseNotFound while a
	// missing collection is ErrCollectionNotFound. The sentinels remain system
	// errors globally because internal refresh/retry paths also use them. The
	// deprecated ErrorCode enum has no database-not-found value, so Code=800 is
	// authoritative there and ErrorCode keeps merr's standard UnexpectedError
	// compatibility fallback.
	err = merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	status := merr.Status(err)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		// Preserve the established SDK-visible collection-not-found message while
		// letting merr.Status own both the typed Code and legacy ErrorCode mapping.
		reason := fmt.Sprintf("can't find collection[database=%s][collection=%s]", database, collectionName)
		status.Reason = reason
		status.Detail = reason
	}
	return status
}

func needsTimestamptzDefaultProjection(field *schemapb.FieldSchema) bool {
	if field.GetDataType() != schemapb.DataType_Timestamptz || field.GetDefaultValue() == nil {
		return false
	}
	_, ok := field.GetDefaultValue().GetData().(*schemapb.ValueField_TimestamptzData)
	return ok
}

// ProjectDescribeCollectionSchema defines the public DescribeCollection schema
// shape shared by cached and remote providers. sourceShared is true for
// MetaCache-owned schemas: only fields that the public TIMESTAMPTZ rewrite will
// mutate are cloned, keeping the canonical cached int64 representation intact.
func ProjectDescribeCollectionSchema(source *schemapb.CollectionSchema, sourceShared bool) (*schemapb.CollectionSchema, error) {
	if source == nil {
		return nil, merr.WrapErrServiceInternalMsg("describe collection returned a nil collection schema")
	}

	projected := &schemapb.CollectionSchema{
		Name:               source.GetName(),
		Description:        source.GetDescription(),
		AutoID:             source.GetAutoID(),
		Fields:             make([]*schemapb.FieldSchema, 0, len(source.GetFields())),
		EnableDynamicField: source.GetEnableDynamicField(),
		Properties:         append([]*commonpb.KeyValuePair(nil), source.GetProperties()...),
		Functions:          append([]*schemapb.FunctionSchema(nil), source.GetFunctions()...),
		DbName:             source.GetDbName(),
		StructArrayFields:  make([]*schemapb.StructArrayFieldSchema, 0, len(source.GetStructArrayFields())),
		Version:            source.GetVersion(),
		ExternalSource:     source.GetExternalSource(),
		ExternalSpec:       source.GetExternalSpec(),
		EnableNamespace:    source.GetEnableNamespace(),
	}

	for _, field := range source.GetFields() {
		if field.GetIsDynamic() || field.GetName() == common.NamespaceFieldName ||
			field.GetFieldID() < common.StartOfUserFieldID {
			continue
		}

		outputField := field
		if sourceShared && needsTimestamptzDefaultProjection(field) {
			outputField = proto.Clone(field).(*schemapb.FieldSchema)
		}
		projected.Fields = append(projected.Fields, outputField)
	}

	// Struct field names are restored in place, so these messages must always be
	// detached from both RootCoord's response and MetaCache's canonical schema.
	for _, field := range source.GetStructArrayFields() {
		projected.StructArrayFields = append(projected.StructArrayFields,
			proto.Clone(field).(*schemapb.StructArrayFieldSchema))
	}

	if err := restoreStructFieldNames(projected); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to restore struct field names")
	}
	return projected, nil
}

func describeCollectionMetadataContext(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Delete(util.HeaderAuthorize)
	return metadata.NewOutgoingContext(ctx, md)
}

func DescribeCollectionRPCContext(ctx context.Context) context.Context {
	return AppendUserInfoForRPC(describeCollectionMetadataContext(ctx))
}
