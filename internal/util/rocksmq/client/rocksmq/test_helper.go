package rocksmq

import (
	"fmt"
	"time"

	server "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}

func newConsumerName() string {
	return fmt.Sprintf("my-consumer-%v", time.Now().Nanosecond())
}

func newMockRocksMQ() server.RocksMQ {
	var rocksmq server.RocksMQ
	return rocksmq
}

func newMockClient() *client {
	client, _ := newClient(ClientOptions{
		Server: newMockRocksMQ(),
	})
	return client
}
