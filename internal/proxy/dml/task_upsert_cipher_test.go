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

package dml

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The message cipher is registered once per process. Re-exec this test so the
// fixture cannot replace, or leak into, another test's cipher registration.
func TestUpsertDeleteMessageCipher(t *testing.T) {
	if os.Getenv("MILVUS_TEST_UPSERT_DELETE_CIPHER") == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestUpsertDeleteMessageCipher$", "-test.v") // #nosec G204 -- Re-exec the current test binary with a fixed test filter.
		cmd.Env = append(os.Environ(), "MILVUS_TEST_UPSERT_DELETE_CIPHER=1")
		output, err := cmd.CombinedOutput()
		t.Logf("%s", output)
		require.NoError(t, err)
		return
	}

	paramtable.Init()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	fixture := &upsertMessageTestCipher{aead: aead}
	message.RegisterCipher(fixture)

	for _, tc := range []struct {
		name           string
		clusterEnabled bool
		collectionEZ   bool
		stringPK       bool
		failGet        bool
		failEncrypt    bool
	}{
		{name: "cmek_int64", clusterEnabled: true, collectionEZ: true},
		{name: "cmek_varchar", clusterEnabled: true, collectionEZ: true, stringPK: true},
		{name: "plain_int64", clusterEnabled: true},
		{name: "plain_varchar", clusterEnabled: true, stringPK: true},
		{name: "cluster_disabled", collectionEZ: true},
		{name: "delete_key_unavailable", clusterEnabled: true, collectionEZ: true, failGet: true},
		{name: "delete_encrypt_failure", clusterEnabled: true, collectionEZ: true, failEncrypt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*fixture = upsertMessageTestCipher{aead: aead, failGet: tc.failGet, failEncrypt: tc.failEncrypt}
			patch := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(tc.clusterEnabled).Build()
			t.Cleanup(func() { patch.UnPatch() })
			allocPatch := mockey.Mock((*allocator.IDAllocator).Alloc).Return(int64(1000), int64(1001), nil).Build()
			t.Cleanup(func() { allocPatch.UnPatch() })

			ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 202}}}}
			field := &schemapb.FieldData{
				FieldId: 100, FieldName: "pk", Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: ids.GetIntId()}}},
			}
			if tc.stringPK {
				ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"private-upsert-pk-alpha", "private-upsert-pk-beta"}}}}
				field.Type = schemapb.DataType_VarChar
				field.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: ids.GetStrId()}}}
			}
			collectionSchema := &schemapb.CollectionSchema{Name: "upsert_cipher", Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: field.Type},
			}}
			if tc.collectionEZ {
				collectionSchema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "7"}}
			}
			schema, err := newSchemaInfo(collectionSchema)
			require.NoError(t, err)
			cache := NewMockCache(t)
			cache.EXPECT().GetPartitionID(mock.Anything, "db", "upsert_cipher", "_default").Return(int64(20), nil)
			channels := channelmgr.NewMockChannelsMgr(t)
			channels.EXPECT().GetVChannels(int64(10)).Return([]string{"test_10v0"}, nil)
			task := &UpsertTask{
				baseTask: baseTask{MetaCache: cache},
				ctx:      context.Background(), req: &milvuspb.UpsertRequest{Base: &commonpb.MsgBase{MsgID: 1}, CollectionName: "upsert_cipher"},
				collectionID: 10, schema: schema, chMgr: channels, idAllocator: &allocator.IDAllocator{}, oldIDs: ids,
				result: &milvuspb.MutationResult{IDs: ids},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{BaseMsg: msgstream.BaseMsg{BeginTimestamp: 12345}, InsertRequest: &msgpb.InsertRequest{
						Base: &commonpb.MsgBase{}, DbName: "db", CollectionName: "upsert_cipher", PartitionName: "_default",
						NumRows: 2, RowIDs: []int64{1, 2}, Timestamps: []uint64{12345, 12345}, FieldsData: []*schemapb.FieldData{field}, Version: msgpb.InsertDataVersion_ColumnBased,
					}},
					DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{DbName: "db", CollectionName: "upsert_cipher", CollectionID: 10, PartitionID: 20, PartitionName: "_default"}},
				},
			}
			task.SetTs(12345)
			wal := mock_streaming.NewMockWALAccesser(t)
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(wal)
			t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
			var persisted []*messagespb.Message
			if !tc.failGet && !tc.failEncrypt {
				wal.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
					func(_ context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
						responses := types.NewAppendResponseN(len(msgs))
						for i, msg := range msgs {
							// WAL backends serialize this representation. Payload() would
							// decrypt first and cannot prove what is actually written.
							persisted = append(persisted, proto.Clone(msg.IntoMessageProto()).(*messagespb.Message))
							responses.FillResponseAtIdx(types.AppendResponse{AppendResult: &types.AppendResult{TimeTick: 12346}}, i)
						}
						return responses
					}).Once()
			}
			err = task.Execute(context.Background())
			if tc.failGet || tc.failEncrypt {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				wal.AssertNotCalled(t, "AppendMessages", mock.Anything, mock.Anything, mock.Anything)
				require.Len(t, fixture.encryptorContexts, 2, "insert must succeed before the delete failure")
				return
			}
			require.NoError(t, err)
			require.Len(t, persisted, 2)
			encrypted := tc.clusterEnabled && tc.collectionEZ
			for _, wire := range persisted {
				restored := message.NewMutableMessageBeforeAppend(wire.Payload, wire.Properties)
				if encrypted {
					if restored.MessageType() == message.MessageTypeDelete {
						plain := &msgpb.DeleteRequest{}
						require.False(t, proto.Unmarshal(wire.Payload, plain) == nil && proto.Equal(ids, plain.PrimaryKeys), "WAL raw delete payload exposes primary keys without decryption")
					}
					require.NotContains(t, string(wire.Payload), "upsert_cipher")
					header := &messagespb.CipherHeader{}
					require.NoError(t, message.DecodeProto(wire.Properties["_ch"], header))
					require.Equal(t, int64(7), header.EzId)
					require.Equal(t, int64(10), header.CollectionId)
					require.Equal(t, []byte("test-edek"), header.SafeKey)
				} else {
					require.NotContains(t, wire.Properties, "_ch")
				}
				switch restored.MessageType() {
				case message.MessageTypeInsert:
					body, err := message.MustAsMutableInsertMessageV1(restored).Body()
					require.NoError(t, err)
					require.True(t, proto.Equal(field, body.FieldsData[0]))
				case message.MessageTypeDelete:
					body, err := message.MustAsMutableDeleteMessageV1(restored).Body()
					require.NoError(t, err)
					require.True(t, proto.Equal(ids, body.PrimaryKeys))
					require.Equal(t, []uint64{12345, 12345}, body.Timestamps)
					require.Equal(t, int64(10), body.CollectionID)
					require.Equal(t, int64(2), body.NumRows)
					plain, err := proto.Marshal(body)
					require.NoError(t, err)
					if encrypted {
						require.NotEqual(t, plain, wire.Payload)
					} else {
						require.Equal(t, plain, wire.Payload)
					}
				default:
					t.Fatalf("unexpected message type %v", restored.MessageType())
				}
			}
			if encrypted {
				require.Equal(t, []message.CipherConfig{{EzID: 7, CollectionID: 10}, {EzID: 7, CollectionID: 10}}, fixture.encryptorContexts)
			} else {
				require.Empty(t, fixture.encryptorContexts)
			}
		})
	}
}

type upsertMessageTestCipher struct {
	hook.Cipher
	aead              cipher.AEAD
	encryptorContexts []message.CipherConfig
	encryptCalls      int
	failGet           bool
	failEncrypt       bool
}

func (c *upsertMessageTestCipher) GetEncryptor(ezID, collectionID int64) (hook.Encryptor, []byte, error) {
	c.encryptorContexts = append(c.encryptorContexts, message.CipherConfig{EzID: ezID, CollectionID: collectionID})
	if c.failGet && len(c.encryptorContexts) == 2 {
		return nil, nil, merr.ErrServiceInternal
	}
	return c, []byte("test-edek"), nil
}

func (c *upsertMessageTestCipher) GetDecryptor(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	if ezID != 7 || collectionID != 10 || string(safeKey) != "test-edek" {
		return nil, merr.ErrServiceInternal
	}
	return c, nil
}

func (c *upsertMessageTestCipher) Encrypt(plain []byte) ([]byte, error) {
	c.encryptCalls++
	if c.failEncrypt && c.encryptCalls == 2 {
		return nil, merr.ErrServiceInternal
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return c.aead.Seal(nonce, nonce, plain, nil), nil
}

func (c *upsertMessageTestCipher) Decrypt(data []byte) ([]byte, error) {
	if len(data) < c.aead.NonceSize() {
		return nil, merr.ErrServiceInternal
	}
	return c.aead.Open(nil, data[:c.aead.NonceSize()], data[c.aead.NonceSize():], nil)
}
