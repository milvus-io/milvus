package partialupdate

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestExtractPKsFromDelete(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		msg := newDeleteMessage(&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{10, 20}},
			},
		})

		pks, ok, err := extractPKs(msg)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []any{int64(10), int64(20)}, pks)
	})

	t.Run("varchar", func(t *testing.T) {
		msg := newDeleteMessage(&schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: []string{"pk-1", "pk-2"}},
			},
		})

		pks, ok, err := extractPKs(msg)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []any{"pk-1", "pk-2"}, pks)
	})

	t.Run("malformed", func(t *testing.T) {
		tests := []struct {
			name string
			ids  *schemapb.IDs
		}{
			{name: "nil_ids"},
			{name: "nil_id_field", ids: &schemapb.IDs{}},
			{
				name: "nil_typed_int_id",
				ids: &schemapb.IDs{
					IdField: (*schemapb.IDs_IntId)(nil),
				},
			},
			{
				name: "nil_int_array",
				ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{},
				},
			},
			{
				name: "empty_int_array",
				ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{},
					},
				},
			},
			{
				name: "nil_string_array",
				ids: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{},
				},
			},
			{
				name: "nil_typed_string_id",
				ids: &schemapb.IDs{
					IdField: (*schemapb.IDs_StrId)(nil),
				},
			},
			{
				name: "empty_string_array",
				ids: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{},
					},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				_, ok, err := extractPKs(newDeleteMessage(test.ids))
				require.True(t, ok)
				requireUnrecoverable(t, err)
			})
		}
	})
}

func TestExtractPKsFromNonRowMessage(t *testing.T) {
	pks, ok, err := extractPKs(newDropCollectionMessage(10))
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, pks)
}

func TestExtractPKsRejectsInvalidMessages(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		pks, ok, err := extractPKs(nil)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, pks)
	})

	t.Run("decode delete", func(t *testing.T) {
		patch := mockey.Mock(message.AsMutableDeleteMessageV1).
			Return(nil, errors.New("decode failed")).
			Build()
		defer patch.UnPatch()

		_, ok, err := extractPKs(newDeleteMessage(&schemapb.IDs{}))
		require.True(t, ok)
		requireUnrecoverable(t, err)
	})

	t.Run("decode delete body", func(t *testing.T) {
		msg := corruptMessageBody(newDeleteMessage(&schemapb.IDs{}))
		_, ok, err := extractPKs(msg)
		require.True(t, ok)
		requireUnrecoverable(t, err)
	})
}

func TestExtractPKsFromInsert(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		pks, err := extractPKsFromInsert(newInsertMessage([]*schemapb.FieldData{
			int64PKFieldData(10, 20),
		}), 100)
		require.NoError(t, err)
		require.Equal(t, []any{int64(10), int64(20)}, pks)
	})

	t.Run("varchar", func(t *testing.T) {
		pks, err := extractPKsFromInsert(newInsertMessage([]*schemapb.FieldData{
			varcharPKFieldData("pk-1", "pk-2"),
		}), 100)
		require.NoError(t, err)
		require.Equal(t, []any{"pk-1", "pk-2"}, pks)
	})

	t.Run("missing_field", func(t *testing.T) {
		_, err := extractPKsFromInsert(newInsertMessage(nil), 100)
		requireUnrecoverable(t, err)
	})

	t.Run("unsupported_type", func(t *testing.T) {
		field := int64PKFieldData(10)
		field.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{}}
		_, err := extractPKsFromInsert(newInsertMessage([]*schemapb.FieldData{field}), 100)
		requireUnrecoverable(t, err)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		_, err := extractPKsFromInsert(nil, 100)
		requireUnrecoverable(t, err)
	})

	t.Run("decode insert", func(t *testing.T) {
		_, err := extractPKsFromInsert(newDeleteMessage(&schemapb.IDs{}), 100)
		requireUnrecoverable(t, err)
	})

	t.Run("decode insert body", func(t *testing.T) {
		_, err := extractPKsFromInsert(corruptMessageBody(newInsertMessage(nil)), 100)
		requireUnrecoverable(t, err)
	})
}

func TestExtractPKsFromCASInsert(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		getter := &staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		}

		pks, scope, err := extractPKsFromCASInsert(newCASInsertMessage(t, []*schemapb.FieldData{
			int64PKFieldData(10, 20),
		}, validCASMeta(100, 1)), getter)

		require.NoError(t, err)
		require.Equal(t, []any{int64(10), int64(20)}, pks)
		require.Equal(t, casInsertScope{collectionID: 10, schemaVersion: 1}, scope)
		require.EqualValues(t, 10, getter.collectionID)
		require.EqualValues(t, 1, getter.schemaVersion)
	})

	t.Run("varchar", func(t *testing.T) {
		getter := &staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_VarChar,
			},
		}

		pks, scope, err := extractPKsFromCASInsert(newCASInsertMessage(t, []*schemapb.FieldData{
			varcharPKFieldData("pk-1", "pk-2"),
		}, validCASMeta(100, 1)), getter)

		require.NoError(t, err)
		require.Equal(t, []any{"pk-1", "pk-2"}, pks)
		require.Equal(t, casInsertScope{collectionID: 10, schemaVersion: 1}, scope)
	})

	t.Run("explicit zero schema version", func(t *testing.T) {
		getter := &staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		}
		msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(100, 1))
		insertMsg := message.MustAsMutableInsertMessageV1(msg)
		header := insertMsg.Header()
		zero := int32(0)
		header.SchemaVersion = &zero
		insertMsg.OverwriteHeader(header)

		pks, scope, err := extractPKsFromCASInsert(msg, getter)

		require.NoError(t, err)
		require.Equal(t, []any{int64(10)}, pks)
		require.Equal(t, casInsertScope{collectionID: 10, schemaVersion: 0}, scope)
		require.EqualValues(t, 0, getter.schemaVersion)
	})
}

func TestExtractPKsFromCASInsertErrors(t *testing.T) {
	validGetter := func() *staticPrimaryKeyDescriptorGetter {
		return &staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		}
	}

	t.Run("missing descriptor getter", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(newInsertMessage(nil), nil)
		requireUnrecoverable(t, err)
	})

	t.Run("decode insert", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(newDeleteMessage(&schemapb.IDs{}), validGetter())
		requireUnrecoverable(t, err)
	})

	t.Run("empty collection", func(t *testing.T) {
		msg := newInsertMessage(nil)
		insertMsg := message.MustAsMutableInsertMessageV1(msg)
		header := insertMsg.Header()
		header.CollectionId = 0
		insertMsg.OverwriteHeader(header)

		_, _, err := extractPKsFromCASInsert(msg, validGetter())
		requireUnrecoverable(t, err)
	})

	t.Run("missing schema version", func(t *testing.T) {
		msg := newInsertMessage(nil)
		insertMsg := message.MustAsMutableInsertMessageV1(msg)
		header := insertMsg.Header()
		header.SchemaVersion = nil
		insertMsg.OverwriteHeader(header)

		_, _, err := extractPKsFromCASInsert(msg, validGetter())
		requireUnrecoverable(t, err)
	})

	t.Run("schema version mismatch", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(newInsertMessage(nil), &staticPrimaryKeyDescriptorGetter{
			err: shards.ErrCollectionSchemaVersionNotMatch,
		})
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsSchemaVersionMismatch())
	})

	t.Run("descriptor lookup", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(newInsertMessage(nil), &staticPrimaryKeyDescriptorGetter{
			err: errors.New("schema lookup failed"),
		})
		requireUnrecoverable(t, err)
	})

	for _, test := range []struct {
		name       string
		descriptor shards.PrimaryKeyDescriptor
	}{
		{
			name: "missing descriptor field",
			descriptor: shards.PrimaryKeyDescriptor{
				DataType: schemapb.DataType_Int64,
			},
		},
		{
			name: "unsupported descriptor type",
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Float,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := extractPKsFromCASInsert(newInsertMessage(nil), &staticPrimaryKeyDescriptorGetter{
				descriptor: test.descriptor,
			})
			requireUnrecoverable(t, err)
		})
	}

	t.Run("decode insert body", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(
			corruptMessageBody(newInsertMessage(nil)),
			validGetter(),
		)
		requireUnrecoverable(t, err)
	})

	t.Run("missing primary key field", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(newInsertMessage(nil), validGetter())
		requireUnrecoverable(t, err)
	})

	t.Run("primary key field type mismatch", func(t *testing.T) {
		_, _, err := extractPKsFromCASInsert(newInsertMessage([]*schemapb.FieldData{
			varcharPKFieldData("pk-1"),
		}), validGetter())
		requireUnrecoverable(t, err)
	})

	t.Run("primary key scalar payload mismatch", func(t *testing.T) {
		field := int64PKFieldData(10)
		field.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"pk-1"}},
				},
			},
		}
		_, _, err := extractPKsFromCASInsert(newInsertMessage([]*schemapb.FieldData{field}), validGetter())
		requireUnrecoverable(t, err)
	})

	t.Run("declared primary key type mismatch", func(t *testing.T) {
		field := int64PKFieldData(10)
		field.Type = schemapb.DataType_VarChar
		_, _, err := extractPKsFromCASInsert(newInsertMessage([]*schemapb.FieldData{field}), validGetter())
		requireUnrecoverable(t, err)
	})
}

func TestExtractPKsFromOrdinaryInsertErrors(t *testing.T) {
	t.Run("missing schema getter", func(t *testing.T) {
		_, _, err := extractPKsFromOrdinaryInsert(newInsertMessage(nil), nil)
		requireUnrecoverable(t, err)
	})

	t.Run("decode insert", func(t *testing.T) {
		expected := errors.New("decode failed")
		patch := mockey.Mock(message.AsMutableInsertMessageV1).Return(nil, expected).Build()
		defer patch.UnPatch()

		_, _, err := extractPKsFromOrdinaryInsert(newInsertMessage(nil), &staticPrimaryKeyDescriptorGetter{})
		requireUnrecoverable(t, err)
	})

	t.Run("legacy insert without collection id", func(t *testing.T) {
		msg := newInsertMessage(nil)
		insert := message.MustAsMutableInsertMessageV1(msg)
		header := insert.Header()
		header.CollectionId = 0
		header.SchemaVersion = nil
		insert.OverwriteHeader(header)

		_, _, err := extractPKsFromOrdinaryInsert(msg, &staticPrimaryKeyDescriptorGetter{
			err: shards.ErrCollectionSchemaNotFound,
		})
		requireUnrecoverable(t, err)
	})

	t.Run("schema lookup", func(t *testing.T) {
		_, _, err := extractPKsFromOrdinaryInsert(newInsertMessage(nil), &staticPrimaryKeyDescriptorGetter{
			err: errors.New("schema lookup failed"),
		})
		requireUnrecoverable(t, err)
	})

	t.Run("unsupported primary key type", func(t *testing.T) {
		_, _, err := extractPKsFromOrdinaryInsert(newInsertMessage(nil), &staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Float,
			},
		})
		requireUnrecoverable(t, err)
	})
}

func TestExtractPKsFromOrdinaryInsertAllowsMissingDeclaredType(t *testing.T) {
	field := int64PKFieldData(10)
	field.Type = schemapb.DataType_None

	pks, fenceCollectionID, err := extractPKsFromOrdinaryInsert(
		newInsertMessage([]*schemapb.FieldData{field}),
		&staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		},
	)

	require.NoError(t, err)
	require.Equal(t, []any{int64(10)}, pks)
	require.Zero(t, fenceCollectionID)
}

func TestExtractCollectionFenceID(t *testing.T) {
	tests := []struct {
		name         string
		msg          message.MutableMessage
		collectionID int64
	}{
		{
			name:         "truncate_collection",
			msg:          newTruncateCollectionMessage(11),
			collectionID: 11,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collectionID, ok := extractCollectionFenceID(test.msg)
			require.True(t, ok)
			require.Equal(t, test.collectionID, collectionID)
		})
	}
}

func TestExtractCollectionFenceIDIgnoresPKAndLifecycleMessages(t *testing.T) {
	for _, msg := range []message.MutableMessage{
		newDropCollectionMessage(10),
		newDropPartitionMessage(10, 20),
		newAlterCollectionMessage(10),
		newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}),
		newDeleteMessage(&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
		}),
	} {
		collectionID, ok := extractCollectionFenceID(msg)
		require.False(t, ok)
		require.Zero(t, collectionID)
	}
}

func TestExtractCollectionFenceIDReturnsEmptyForMalformedMessages(t *testing.T) {
	tests := []struct {
		name string
		msg  message.MutableMessage
	}{
		{
			name: "truncate_collection_zero_collection_id",
			msg:  newTruncateCollectionMessage(0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collectionID, ok := extractCollectionFenceID(test.msg)
			require.True(t, ok)
			require.Zero(t, collectionID)
		})
	}
}

func TestExtractCollectionFenceIDHandlesDecodeErrors(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		collectionID, ok := extractCollectionFenceID(nil)
		require.False(t, ok)
		require.Zero(t, collectionID)
	})

	t.Run("truncate", func(t *testing.T) {
		patch := mockey.Mock(message.AsMutableTruncateCollectionMessageV2).
			Return(nil, errors.New("decode failed")).
			Build()
		defer patch.UnPatch()

		collectionID, ok := extractCollectionFenceID(newTruncateCollectionMessage(10))
		require.True(t, ok)
		require.Zero(t, collectionID)
	})
}

func TestExtractCollectionFenceIDFromNonFenceMessage(t *testing.T) {
	collectionID, ok := extractCollectionFenceID(newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}))
	require.False(t, ok)
	require.Zero(t, collectionID)
}

func TestExtractDropCollectionIDReturnsEmptyForMalformedMessage(t *testing.T) {
	patch := mockey.Mock(message.AsMutableDropCollectionMessageV1).
		Return(nil, errors.New("decode failed")).
		Build()
	defer patch.UnPatch()

	collectionID, ok := extractDropCollectionID(newDropCollectionMessage(10))
	require.True(t, ok)
	require.Zero(t, collectionID)
}

type staticPrimaryKeyDescriptorGetter struct {
	descriptor    shards.PrimaryKeyDescriptor
	err           error
	collectionID  int64
	schemaVersion int32
}

func corruptMessageBody(msg message.MutableMessage) message.MutableMessage {
	properties := make(map[string]string, len(msg.Properties().ToRawMap()))
	for key, value := range msg.Properties().ToRawMap() {
		properties[key] = value
	}
	return message.NewMutableMessageBeforeAppend([]byte{0xff}, properties)
}

func (g *staticPrimaryKeyDescriptorGetter) GetPrimaryKeyDescriptor(collectionID int64, schemaVersion int32) (shards.PrimaryKeyDescriptor, error) {
	g.collectionID = collectionID
	g.schemaVersion = schemaVersion
	return g.descriptor, g.err
}

func newDeleteMessage(ids *schemapb.IDs) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 10,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			PrimaryKeys: ids,
		}).
		MustBuildMutable()
}

func newInsertMessage(fields []*schemapb.FieldData) message.MutableMessage {
	schemaVersion := int32(1)
	return message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId:  10,
			SchemaVersion: &schemaVersion,
		}).
		WithBody(&msgpb.InsertRequest{
			FieldsData: fields,
		}).
		MustBuildMutable()
}

func newDropCollectionMessage(collectionID int64) message.MutableMessage {
	return message.NewDropCollectionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable()
}

func newDropPartitionMessage(collectionID int64, partitionID int64) message.MutableMessage {
	return message.NewDropPartitionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: collectionID,
			PartitionId:  partitionID,
		}).
		WithBody(&msgpb.DropPartitionRequest{}).
		MustBuildMutable()
}

func newTruncateCollectionMessage(collectionID int64) message.MutableMessage {
	return message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable()
}

func newAlterCollectionMessage(collectionID int64) message.MutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&message.AlterCollectionMessageBody{}).
		MustBuildMutable()
}

func int64PKFieldData(values ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "pk",
		FieldId:   100,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: values},
				},
			},
		},
	}
}

func varcharPKFieldData(values ...string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "pk",
		FieldId:   100,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: values},
				},
			},
		},
	}
}
