package message

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPartialUpdateCASPropertyRoundTrip(t *testing.T) {
	meta := validPartialUpdateCAS()
	msg := newPartialUpdateCASTestMessageWithMeta(t, meta)

	got, err := ExtractPartialUpdateCAS(msg)
	require.NoError(t, err)
	require.True(t, proto.Equal(meta, got))
	require.True(t, HasPartialUpdateCAS(msg))
}

func TestPartialUpdateCASMetadataStoredInBody(t *testing.T) {
	meta := validPartialUpdateCAS()
	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{}})
	require.NoError(t, builder.AddPartialUpdateCAS(meta))
	msg := builder.MustBuildMutable()

	marker, ok := msg.Properties().Get(messagePartialUpdateCAS)
	require.True(t, ok)
	require.Empty(t, marker)

	insertMsg, err := AsMutableInsertMessageV1(msg)
	require.NoError(t, err)
	body, err := insertMsg.Body()
	require.NoError(t, err)
	encoded := body.GetBase().GetProperties()[messagePartialUpdateCAS]
	require.NotEmpty(t, encoded)
	decoded := &messagespb.PartialUpdateCAS{}
	require.NoError(t, DecodeProto(encoded, decoded))
	require.True(t, proto.Equal(meta, decoded))
}

func TestPartialUpdateCASBuilderEncryptsMetadataWithBody(t *testing.T) {
	oldCipher := cipher
	cipher = partialUpdateCASTestCipher{}
	t.Cleanup(func() { cipher = oldCipher })

	meta := validPartialUpdateCAS()
	body := &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}
	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBody(body)
	require.NoError(t, builder.AddPartialUpdateCAS(meta))
	plainBody, err := proto.Marshal(body)
	require.NoError(t, err)
	encodedMeta, err := EncodeProto(meta)
	require.NoError(t, err)

	msg, err := builder.
		WithCipher(&CipherConfig{EzID: 1, CollectionID: 10}).
		BuildMutable()
	require.NoError(t, err)
	rawPayload := msg.IntoMessageProto().GetPayload()
	require.NotEqual(t, plainBody, rawPayload)
	require.False(t, bytes.Contains(rawPayload, []byte(encodedMeta)))

	got, err := ExtractPartialUpdateCAS(msg)
	require.NoError(t, err)
	require.True(t, proto.Equal(meta, got))
}

func TestPartialUpdateCASBodyEncoderEncryptsMetadataWithBody(t *testing.T) {
	oldCipher := cipher
	cipher = partialUpdateCASTestCipher{}
	t.Cleanup(func() { cipher = oldCipher })

	meta := validPartialUpdateCAS()
	template := &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}
	// CAS must be part of the template before the encoder plans its exact size.
	require.NoError(t, EncodePartialUpdateCASIntoInsertTemplate(meta, template))
	encodedBody, err := proto.Marshal(template)
	require.NoError(t, err)
	encoder := &partialUpdateCASTestBodyEncoder{payload: encodedBody}

	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBodyEncoder(encoder)
	require.NoError(t, builder.MarkPartialUpdateCASForBodyEncoder())
	msg, err := builder.
		WithCipher(&CipherConfig{EzID: 1, CollectionID: 10}).
		BuildMutable()
	require.NoError(t, err)
	require.Equal(t, 1, encoder.encodedSizeCalls)
	require.Equal(t, 1, encoder.marshalToCalls)

	marker, ok := msg.Properties().Get(messagePartialUpdateCAS)
	require.True(t, ok)
	require.Empty(t, marker)
	rawPayload := msg.IntoMessageProto().GetPayload()
	require.NotEqual(t, encodedBody, rawPayload)
	encodedMeta := template.GetBase().GetProperties()[messagePartialUpdateCAS]
	require.NotEmpty(t, encodedMeta)
	require.False(t, bytes.Contains(rawPayload, []byte(encodedMeta)))

	got, err := ExtractPartialUpdateCAS(msg)
	require.NoError(t, err)
	require.True(t, proto.Equal(meta, got))
}

func TestMarkPartialUpdateCASCommit(t *testing.T) {
	commit := NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&CommitTxnMessageHeader{}).
		WithBody(&CommitTxnMessageBody{}).
		MustBuildMutable()

	require.NoError(t, MarkPartialUpdateCASCommit(commit))
	require.True(t, HasPartialUpdateCAS(commit))
	marker, ok := commit.Properties().Get(messagePartialUpdateCAS)
	require.True(t, ok)
	require.Empty(t, marker)

	require.Error(t, MarkPartialUpdateCASCommit(newPartialUpdateCASTestMessage()))
	require.Error(t, MarkPartialUpdateCASCommit(nil))
}

func TestPartialUpdateCASDefensiveErrors(t *testing.T) {
	t.Run("invalid builder metadata", func(t *testing.T) {
		builder := NewInsertMessageBuilderV1().
			WithVChannel("v1").
			WithHeader(&InsertMessageHeader{CollectionId: 10}).
			WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{}})
		require.Error(t, builder.AddPartialUpdateCAS(&messagespb.PartialUpdateCAS{ReadTs: 100}))
	})

	t.Run("non insert builder", func(t *testing.T) {
		builder := NewCommitTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&CommitTxnMessageHeader{}).
			WithBody(&CommitTxnMessageBody{})
		require.Error(t, builder.AddPartialUpdateCAS(validPartialUpdateCAS()))
	})

	t.Run("nil body encoder template", func(t *testing.T) {
		require.Error(t, EncodePartialUpdateCASIntoInsertTemplate(validPartialUpdateCAS(), nil))
	})

	t.Run("body encoder marker without encoder", func(t *testing.T) {
		builder := NewInsertMessageBuilderV1().
			WithVChannel("v1").
			WithHeader(&InsertMessageHeader{CollectionId: 10})
		require.Error(t, builder.MarkPartialUpdateCASForBodyEncoder())
	})

	t.Run("body encoder marker on non insert", func(t *testing.T) {
		builder := NewDeleteMessageBuilderV1().
			WithVChannel("v1").
			WithHeader(&DeleteMessageHeader{}).
			WithBodyEncoder(partialUpdateCASTestDeleteBodyEncoder{})
		require.Error(t, builder.MarkPartialUpdateCASForBodyEncoder())
	})

	t.Run("commit without property setter", func(t *testing.T) {
		commit := NewCommitTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&CommitTxnMessageHeader{}).
			WithBody(&CommitTxnMessageBody{}).
			MustBuildMutable()
		wrappedCommit := struct{ MutableMessage }{commit}
		require.Error(t, MarkPartialUpdateCASCommit(wrappedCommit))
	})
}

func TestPartialUpdateCASPropertyMissing(t *testing.T) {
	msg := newPartialUpdateCASTestMessage()

	got, err := ExtractPartialUpdateCAS(msg)
	require.NoError(t, err)
	require.Nil(t, got)
	require.False(t, HasPartialUpdateCAS(msg))
}

func TestPartialUpdateCASPropertyMalformed(t *testing.T) {
	msg := newPartialUpdateCASTestMessage("not-base64")

	got, err := ExtractPartialUpdateCAS(msg)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Nil(t, got)
}

func TestPartialUpdateCASPropertyIncomplete(t *testing.T) {
	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{}})
	err := builder.AddPartialUpdateCAS(&messagespb.PartialUpdateCAS{ReadTs: 100})
	require.Error(t, err)

	invalid := validPartialUpdateCAS()
	invalid.ObservedPchannelTerm = -1
	err = builder.AddPartialUpdateCAS(invalid)
	require.Error(t, err)
}

func TestPartialUpdateCASPropertyNilMeta(t *testing.T) {
	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{}})
	err := builder.AddPartialUpdateCAS(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestPartialUpdateCASPropertyValidation(t *testing.T) {
	tests := []struct {
		name    string
		meta    func() *messagespb.PartialUpdateCAS
		wantErr bool
	}{
		{
			name: "zero_read_ts",
			meta: func() *messagespb.PartialUpdateCAS {
				meta := validPartialUpdateCAS()
				meta.ReadTs = 0
				return meta
			},
			wantErr: true,
		},
		{
			name: "zero_observed_term",
			meta: func() *messagespb.PartialUpdateCAS {
				meta := validPartialUpdateCAS()
				meta.ObservedPchannelTerm = 0
				return meta
			},
			wantErr: true,
		},
		{
			name: "valid_attempt_proof",
			meta: validPartialUpdateCAS,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewInsertMessageBuilderV1().
				WithVChannel("v1").
				WithHeader(&InsertMessageHeader{CollectionId: 10}).
				WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{}})
			err := builder.AddPartialUpdateCAS(test.meta())
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestPartialUpdateCASPropertyImmutableExtraction(t *testing.T) {
	meta := validPartialUpdateCAS()
	msg := newPartialUpdateCASTestMessageWithMeta(t, meta)
	immutable := msg.IntoImmutableMessage(nil)

	got, err := ExtractPartialUpdateCAS(immutable)
	require.NoError(t, err)
	require.True(t, proto.Equal(meta, got))
	require.True(t, HasPartialUpdateCAS(immutable))
}

func TestPartialUpdateCASPropertyInvalidProtoWire(t *testing.T) {
	msg := newPartialUpdateCASTestMessage(base64.StdEncoding.EncodeToString([]byte{0xff, 0xff, 0xff}))

	got, err := ExtractPartialUpdateCAS(msg)
	require.Error(t, err)
	require.Nil(t, got)
}

func TestPartialUpdateCASPropertyExtractSemanticallyInvalid(t *testing.T) {
	encoded, err := EncodeProto(&messagespb.PartialUpdateCAS{ReadTs: 100})
	require.NoError(t, err)
	msg := newPartialUpdateCASTestMessage(encoded)

	got, err := ExtractPartialUpdateCAS(msg)
	require.Error(t, err)
	require.Nil(t, got)
}

func newPartialUpdateCASTestMessage(propertyValue ...string) MutableMessage {
	body := &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}
	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBody(body)
	if len(propertyValue) > 0 {
		body.Base.Properties = map[string]string{messagePartialUpdateCAS: propertyValue[0]}
		builder.WithBody(body).WithProperty(messagePartialUpdateCAS, "")
	}
	return builder.MustBuildMutable()
}

func newPartialUpdateCASTestMessageWithMeta(t *testing.T, meta *messagespb.PartialUpdateCAS) MutableMessage {
	t.Helper()
	builder := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 10}).
		WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{}})
	require.NoError(t, builder.AddPartialUpdateCAS(meta))
	return builder.MustBuildMutable()
}

func validPartialUpdateCAS() *messagespb.PartialUpdateCAS {
	return &messagespb.PartialUpdateCAS{
		ReadTs:               100,
		ObservedPchannelTerm: 2,
	}
}

type partialUpdateCASTestCipher struct{}

func (partialUpdateCASTestCipher) Init(map[string]string) error {
	return nil
}

func (partialUpdateCASTestCipher) GetEncryptor(int64, int64) (hook.Encryptor, []byte, error) {
	return partialUpdateCASTestCryptor{}, []byte("safe-key"), nil
}

func (partialUpdateCASTestCipher) GetDecryptor(int64, int64, []byte) (hook.Decryptor, error) {
	return partialUpdateCASTestCryptor{}, nil
}

func (partialUpdateCASTestCipher) GetUnsafeKey(int64, int64) []byte {
	return nil
}

type partialUpdateCASTestCryptor struct{}

func (partialUpdateCASTestCryptor) Encrypt(plainText []byte) ([]byte, error) {
	return partialUpdateCASTestXOR(plainText), nil
}

func (partialUpdateCASTestCryptor) Decrypt(cipherText []byte) ([]byte, error) {
	return partialUpdateCASTestXOR(cipherText), nil
}

func partialUpdateCASTestXOR(input []byte) []byte {
	output := make([]byte, len(input))
	for i, value := range input {
		output[i] = value ^ 0xff
	}
	return output
}

type partialUpdateCASTestBodyEncoder struct {
	payload          []byte
	encodedSizeCalls int
	marshalToCalls   int
}

func (e *partialUpdateCASTestBodyEncoder) EncodedSize() (int, error) {
	e.encodedSizeCalls++
	return len(e.payload), nil
}

func (e *partialUpdateCASTestBodyEncoder) MarshalTo(dst []byte) (int, error) {
	e.marshalToCalls++
	return copy(dst, e.payload), nil
}

func (e *partialUpdateCASTestBodyEncoder) BodyType() *msgpb.InsertRequest {
	return nil
}

type partialUpdateCASTestDeleteBodyEncoder struct{}

func (partialUpdateCASTestDeleteBodyEncoder) EncodedSize() (int, error) {
	return 0, nil
}

func (partialUpdateCASTestDeleteBodyEncoder) MarshalTo([]byte) (int, error) {
	return 0, nil
}

func (partialUpdateCASTestDeleteBodyEncoder) BodyType() *msgpb.DeleteRequest {
	return nil
}
