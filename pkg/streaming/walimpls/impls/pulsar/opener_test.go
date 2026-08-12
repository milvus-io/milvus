package pulsar

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testPulsarTopicAdmin struct {
	topic       string
	err         error
	hasDeadline bool
	deadline    time.Time
}

func (a *testPulsarTopicAdmin) GetStatsWithContext(
	ctx context.Context,
	topic utils.TopicName,
) (utils.TopicStats, error) {
	a.topic = topic.String()
	a.deadline, a.hasDeadline = ctx.Deadline()
	return utils.TopicStats{}, a.err
}

func TestOpenReadOnlyWALChecksTopicExistence(t *testing.T) {
	topicAdmin := &testPulsarTopicAdmin{
		err: rest.Error{Code: http.StatusNotFound, Reason: "Topic not found"},
	}
	opener := &openerImpl{
		tenant:     tenant{tenant: "public", namespace: "default"},
		topicAdmin: topicAdmin,
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
		tenant:     tenant{tenant: "public", namespace: "default"},
		topicAdmin: topicAdmin,
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

func TestOpenReadOnlyWALRejectsInconclusiveTopicCheck(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{
			name: "generic 404 from ingress",
			err:  rest.Error{Code: http.StatusNotFound, Reason: "404 page not found"},
		},
		{
			name: "admin unauthorized",
			err:  rest.Error{Code: http.StatusUnauthorized, Reason: "Unauthorized"},
		},
		{
			name: "admin timeout",
			err:  context.DeadlineExceeded,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			topicAdmin := &testPulsarTopicAdmin{err: testCase.err}
			opener := &openerImpl{
				tenant:     tenant{tenant: "public", namespace: "default"},
				topicAdmin: topicAdmin,
			}

			wal, err := opener.Open(context.Background(), &walimpls.OpenOption{
				Channel: types.PChannelInfo{
					Name:       "existing-historical-topic",
					AccessMode: types.AccessModeRO,
				},
			})
			require.Nil(t, wal)
			require.ErrorIs(t, err, merr.ErrMqInternal)
			require.NotErrorIs(t, err, merr.ErrMqTopicNotFound)
			require.True(t, topicAdmin.hasDeadline)
			require.WithinDuration(t, time.Now().Add(pulsarTopicCheckTimeout), topicAdmin.deadline, time.Second)
		})
	}
}

func TestOpenReadOnlyWALRejectsTopicAdminCreationFailure(t *testing.T) {
	opener := &openerImpl{
		tenant: tenant{tenant: "public", namespace: "default"},
		newTopicAdmin: func() (pulsarTopicAdmin, error) {
			return nil, merr.WrapErrMqInternal(errors.New("admin client is unavailable"))
		},
	}

	wal, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "existing-historical-topic",
			AccessMode: types.AccessModeRO,
		},
	})
	require.Nil(t, wal)
	require.ErrorIs(t, err, merr.ErrMqInternal)
	require.NotErrorIs(t, err, merr.ErrMqTopicNotFound)
}

func TestOpenReadOnlyWALPreservesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	opener := &openerImpl{tenant: tenant{tenant: "public", namespace: "default"}}
	wal, err := opener.Open(ctx, &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "historical-topic",
			AccessMode: types.AccessModeRO,
		},
	})
	require.Nil(t, wal)
	require.ErrorIs(t, err, context.Canceled)
}

func TestExplicitPulsarTopicNotFound(t *testing.T) {
	require.True(t, isExplicitPulsarTopicNotFound(
		rest.Error{Code: http.StatusNotFound, Reason: "Topic not found"},
	))
	require.True(t, isExplicitPulsarTopicNotFound(
		rest.Error{Code: http.StatusNotFound, Reason: "topic persistent://public/default/test does not exist"},
	))
	require.False(t, isExplicitPulsarTopicNotFound(
		rest.Error{Code: http.StatusNotFound, Reason: "404 page not found"},
	))
	require.False(t, isExplicitPulsarTopicNotFound(
		rest.Error{Code: http.StatusUnauthorized, Reason: "Topic not found"},
	))
}
