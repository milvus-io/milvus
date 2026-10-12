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

package proxy

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Reply lookup outcomes reported to API callers.
const (
	replyStatusDone    = "done"
	replyStatusPending = "pending"
)

// defaultCommandTTLSeconds bounds how long a one-time command pushed over HTTP without a
// ttl_seconds survives, so a command nobody ever collects is eventually reclaimed instead
// of occupying RootCoord memory for the life of the process.
//
// It is a bound on memory, not a delivery window, and deliberately does not encode "N
// heartbeat cycles": HeartbeatInterval is client-side config with no upper bound and the
// server is never told what it is.
//
// The default lives here, not in the store, because this is the only layer that can see the
// difference between "no ttl_seconds" and "ttl_seconds: 0". On the RPC it is invisible even
// with an optional field: proto3 implicit presence means a client built against the older
// definition emits nothing at all for an explicit 0, so the server cannot distinguish that
// deliberate "never expire" from an unspecified field. Defaulting there would silently give
// every such client a one-hour expiry it never asked for.
const defaultCommandTTLSeconds = 3600

// resolveCommandTTL applies the HTTP default to an omitted ttl_seconds.
//
//	absent -> defaultCommandTTLSeconds
//	0      -> 0, an explicit "never expire"
//	other  -> honored verbatim (negative also means never expire)
func resolveCommandTTL(requested *int64) int64 {
	if requested == nil {
		return defaultCommandTTLSeconds
	}
	return *requested
}

// clientCommandReply pairs a reply with the client that produced it.
//
// A command that names no target is stored with scope "global" and delivered to every
// connected client, so one command ID can have many answers. Reporting a bare reply would
// force the reader to guess which client it came from -- or worse, read one client's answer
// as the cluster's.
type clientCommandReply struct {
	ClientID string        `json:"client_id"`
	Reply    *CommandReply `json:"reply"`
}

// findCommandReplies looks up every reply to a command. clientID may be empty, in which
// case every known client is scanned; passing it turns the lookup into a targeted one,
// which is what callers who pushed a client-scoped command should do.
//
// It returns the replies, each tagged with the client that produced it, and how many
// clients this lookup examined. That count is an observation, not a delivery target: the
// scan covers every cached client regardless of the command's scope, so it establishes
// nothing about completeness either way. See commandReplyPayload.
//
// Replies are ordered by client ID so that repeating the request yields the same order --
// the underlying iteration is over a sync.Map, whose order is unspecified.
//
// An empty result with a nil error means the command has not been answered yet -- a normal
// state, not a failure, because replies only arrive on a client's next heartbeat.
func findCommandReplies(ctx context.Context, node *Proxy, clientID, commandID string) ([]clientCommandReply, int, error) {
	// Metrics are the largest thing a client reports, so skip them: they are not needed to
	// read replies. It does not make the lookup cheap -- command_replies is encoded into
	// ClientInfo.Reserved regardless of this flag, so the response still carries each
	// matching client's whole reply history. Passing clientID is what actually bounds it.
	resp, err := node.GetClientTelemetry(ctx, &milvuspb.GetClientTelemetryRequest{
		ClientId:       clientID,
		IncludeMetrics: false,
	})
	if err != nil {
		return nil, 0, err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, 0, err
	}

	clients := ConvertClientTelemetryResponse(resp).Clients

	var found []clientCommandReply
	for _, client := range clients {
		for _, reply := range client.CommandReplies {
			if reply.CommandID == commandID {
				found = append(found, clientCommandReply{ClientID: client.ClientID, Reply: reply})
				// One client answers a given command at most once.
				break
			}
		}
	}

	sort.Slice(found, func(i, j int) bool { return found[i].ClientID < found[j].ClientID })
	return found, len(clients), nil
}

// commandReplyPayload renders a reply lookup as the JSON body shared by every endpoint
// that can return one, so callers can parse a single shape regardless of which endpoint
// produced it.
//
// `status` is always present: "done" once at least one client has answered, "pending"
// otherwise. Callers should branch on it rather than on the presence of `reply`. A
// "pending" response is not an error -- the client answers on its next heartbeat, so the
// caller polls this endpoint again when it is ready to.
//
// `replies` is always an array, one entry per answering client. Re-querying later returns
// everything accumulated so far -- replies are retained per client (the most recent 50).
//
// `observed_clients` is how many clients this lookup examined, not how many the command was
// aimed at, and the two are not the same: the scan covers every cached client regardless of
// the command's target scope, and includes clients that have gone inactive or connected
// after the command was pushed. So `responded` reaching `observed_clients` does not mean
// every recipient answered, and falling short of it does not mean any are missing. There is
// deliberately no field claiming completeness, because the server does not record who a
// broadcast command was delivered to; treat both numbers as observations.
//
// `reply` and `client_id` repeat the first entry, which is the whole answer for the common
// case of a command aimed at one client. Anything reading a command that was not
// client-scoped must use `replies`. When nothing has answered yet, `client_id` falls back
// to targetClientID so a caller that pushed to a known client still sees who it is waiting
// on; it is omitted when the command named no target.
func commandReplyPayload(commandID, targetClientID string, replies []clientCommandReply, observedClients int) map[string]interface{} {
	if replies == nil {
		replies = []clientCommandReply{}
	}

	body := map[string]interface{}{
		"command_id":       commandID,
		"status":           replyStatusPending,
		"replies":          replies,
		"responded":        len(replies),
		"observed_clients": observedClients,
	}
	if len(replies) == 0 {
		if targetClientID != "" {
			body["client_id"] = targetClientID
		}
		body["message"] = "Command has not been answered yet. The client replies on its " +
			"next heartbeat; query this endpoint again to collect it."
		return body
	}

	body["status"] = replyStatusDone
	body["client_id"] = replies[0].ClientID
	body["reply"] = replies[0].Reply
	return body
}
