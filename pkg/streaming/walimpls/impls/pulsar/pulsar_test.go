package pulsar

import (
	"net/http"
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestTenant(t *testing.T) {
	tenant := tenant{
		tenant:    "milvus",
		namespace: "aaa",
	}
	assert.Equal(t, "milvus/aaa/test", tenant.MustGetFullTopicName("test"))
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(message.WALNamePulsar)
	assert.NotNil(t, registeredB)
	assert.Equal(t, message.WALNamePulsar, registeredB.Name())

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{
		WALName: commonpb.WALName(message.WALNamePulsar),
		Id:      newMessageIDOfPulsar(1, 2, 3).Marshal(),
	})
	assert.NoError(t, err)
	assert.True(t, id.EQ(newMessageIDOfPulsar(1, 2, 3)))
}

func TestPulsar(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
}

func TestMapPulsarReadError(t *testing.T) {
	topicNotFound := errors.New("TopicNotFound: historical topic does not exist")
	assert.ErrorIs(t, mapPulsarReadError("missing-topic", topicNotFound), merr.ErrMqTopicNotFound)

	transient := errors.New("transient read error")
	assert.ErrorIs(t, mapPulsarReadError("topic", transient), transient)
}

func TestMapPulsarAdminReadError(t *testing.T) {
	notFound := rest.Error{Code: http.StatusNotFound, Reason: "historical topic does not exist"}
	assert.ErrorIs(t, mapPulsarAdminReadError("missing-topic", notFound), merr.ErrMqTopicNotFound)

	transient := rest.Error{Code: http.StatusServiceUnavailable, Reason: "admin service unavailable"}
	assert.ErrorIs(t, mapPulsarAdminReadError("topic", transient), transient)
	assert.NoError(t, mapPulsarAdminReadError("topic", nil))
}
