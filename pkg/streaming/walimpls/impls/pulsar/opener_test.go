package pulsar

import (
	"context"
	"net/http"
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testPulsarTopicAdmin struct {
	topic string
	err   error
}

func (a *testPulsarTopicAdmin) GetStatsWithContext(
	_ context.Context,
	topic utils.TopicName,
) (utils.TopicStats, error) {
	a.topic = topic.String()
	return utils.TopicStats{}, a.err
}

func TestOpenReadOnlyWALChecksTopicExistence(t *testing.T) {
	topicAdmin := &testPulsarTopicAdmin{
		err: rest.Error{Code: http.StatusNotFound, Reason: "Topic not found"},
	}
	opener := &openerImpl{
		tenant: tenant{tenant: "public", namespace: "default"},
		topics: topicAdmin,
	}

	wal, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "missing-historical-topic",
			AccessMode: types.AccessModeRO,
		},
	})
	require.Nil(t, wal)
	require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	require.Equal(t, "persistent://public/default/missing-historical-topic", topicAdmin.topic)
}

func TestOpenReadOnlyWALAllowsExistingTopic(t *testing.T) {
	topicAdmin := &testPulsarTopicAdmin{}
	opener := &openerImpl{
		tenant: tenant{tenant: "public", namespace: "default"},
		topics: topicAdmin,
	}

	wal, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "existing-historical-topic",
			AccessMode: types.AccessModeRO,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, wal)
	require.Equal(t, "persistent://public/default/existing-historical-topic", topicAdmin.topic)
	wal.Close()
}
