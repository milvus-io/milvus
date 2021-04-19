package reader

import (
	"context"
	"github.com/czs007/suvlim/internal/conf"
	"strconv"
	"testing"
	"time"
)

const ctxTimeInMillisecond = 10

// NOTE: start pulsar before test
func TestReader_startQueryNode(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	StartQueryNode(ctx, pulsarAddr)
}
