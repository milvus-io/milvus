package handler

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	lifetime        *typeutil.Lifetime
	rb              resolver.Builder
	queryViewClient *queryViewClient
}

func (c *clientImpl) QueryViewClient() QueryViewClient {
	return c.queryViewClient
}

func (c *clientImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.lifetime.Wait()
	c.queryViewClient.close()
	if c.rb != nil {
		c.rb.Close()
	}
}
