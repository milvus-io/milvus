package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestBroadcastAppendResult_GetAppendResult(t *testing.T) {
	msgID := mock_message.NewMockMessageID(t)
	result := &BroadcastAppendResult{
		AppendResults: map[string]*AppendResult{
			"channel1": {MessageID: msgID},
		},
	}
	appendResult := result.GetAppendResult("channel1")
	assert.NotNil(t, appendResult)
	assert.NotNil(t, appendResult.MessageID)
}

func TestAppendResult_GetExtra(t *testing.T) {
	extra, err := anypb.New(&messagespb.TxnContext{TxnId: 1})
	assert.NoError(t, err)

	result := &AppendResult{
		Extra: extra,
	}

	var txnCtx messagespb.TxnContext
	err = result.GetExtra(&txnCtx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), txnCtx.TxnId)
}

func TestAppendResult_IntoProto(t *testing.T) {
	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().Marshal().Return("1")
	result := &AppendResult{
		MessageID: msgID,
		TimeTick:  12345,
		TxnCtx:    &message.TxnContext{TxnID: 1},
	}

	protoResult := result.IntoProto()
	assert.NotNil(t, protoResult)
	assert.Equal(t, "1", protoResult.Id.Id)
	assert.Equal(t, uint64(12345), protoResult.Timetick)
	assert.Equal(t, int64(1), protoResult.TxnContext.TxnId)
}

func TestAppendResponses_MaxTimeTick(t *testing.T) {
	responses := AppendResponses{
		Responses: []AppendResponse{
			{AppendResult: &AppendResult{TimeTick: 100}},
			{AppendResult: &AppendResult{TimeTick: 200}},
		},
	}

	maxTimeTick := responses.MaxTimeTick()
	assert.Equal(t, uint64(200), maxTimeTick)
}

func TestAppendResponses_UnwrapFirstError(t *testing.T) {
	err := assert.AnError
	responses := AppendResponses{
		Responses: []AppendResponse{
			{Error: err},
			{Error: nil},
		},
	}

	firstError := responses.UnwrapFirstError()
	assert.Equal(t, err, firstError)
}

func TestAppendResponses_FillAllError(t *testing.T) {
	err := assert.AnError
	responses := AppendResponses{
		Responses: make([]AppendResponse, 2),
	}

	responses.FillAllError(err)
	for _, resp := range responses.Responses {
		assert.Equal(t, err, resp.Error)
	}
}

func TestAppendResponses_FillResponseAtIdx(t *testing.T) {
	resp := AppendResponse{AppendResult: &AppendResult{TimeTick: 100}}
	responses := AppendResponses{
		Responses: make([]AppendResponse, 2),
	}

	responses.FillResponseAtIdx(resp, 1)
	assert.Equal(t, resp, responses.Responses[1])
}

func TestAppendResponses_FillAllResponse(t *testing.T) {
	resp := AppendResponse{AppendResult: &AppendResult{TimeTick: 100}}
	responses := AppendResponses{
		Responses: make([]AppendResponse, 2),
	}

	responses.FillAllResponse(resp)
	for _, r := range responses.Responses {
		assert.Equal(t, resp, r)
	}
}
