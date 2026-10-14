package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastCollectionAlter rejects unsupported schema evolution before WAL
// admission. Worker sync streams independently attest the protocol, so a node
// joining after this snapshot cannot acknowledge readiness with old semantics.
func (c *Core) broadcastCollectionAlter(ctx context.Context, api broadcaster.BroadcastAPI, msg message.BroadcastMutableMessage) error {
	if messageutil.IsSchemaChange(message.MustAsMutableAlterCollectionMessageV2(msg).Header()) {
		if err := c.validateSchemaProtocolParticipants(ctx); err != nil {
			return err
		}
	}
	_, err := api.Broadcast(ctx, msg)
	return err
}

func (c *Core) validateSchemaProtocolParticipants(ctx context.Context) error {
	for _, role := range []string{typeutil.ProxyRole, typeutil.StreamingNodeRole, typeutil.QueryNodeRole} {
		nodes, _, err := c.session.GetSessions(ctx, role)
		if err != nil {
			return merr.Wrapf(err, "check %s schema protocol capability", role)
		}
		for _, node := range nodes {
			if node.QueryViewSchemaProtocol != common.QueryViewSchemaProtocol {
				return merr.WrapErrServiceUnavailableMsg("schema evolution requires QueryView schema protocol %s; %s node %d advertises %q",
					common.QueryViewSchemaProtocol, role, node.ServerID, node.QueryViewSchemaProtocol)
			}
		}
	}
	return nil
}
