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
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// maxTelemetryWait bounds how long a synchronous telemetry request may block. A client
	// answers on its next heartbeat, so anything beyond a couple of heartbeat intervals is
	// almost certainly a client that is not going to answer at all.
	maxTelemetryWait = 90 * time.Second

	// telemetryReplyPollInterval is how often the proxy re-reads client state while waiting
	// for a reply. Replies only ever land on a heartbeat -- 30s by default -- so polling
	// faster than this buys nothing and is not free: every poll ships the target client's
	// entire stored reply set (up to 50 replies, each with a payload capped at 1MiB) from
	// RootCoord through the proxy, because command_replies is encoded into
	// ClientInfo.Reserved regardless of IncludeMetrics.
	telemetryReplyPollInterval = 2 * time.Second
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
// It is a bound on memory, not a delivery window, and deliberately does not try to encode
// "N heartbeat cycles": HeartbeatInterval is client-side config with no upper bound, the
// server is not told what it is, and clients matched by one scope may use different values.
// An hour covers a client on a multi-minute interval, or one briefly disconnected.
//
// The default lives here rather than in the store because this is the only layer that can
// see the difference between "no ttl_seconds" and "ttl_seconds: 0". The RPC and the store
// keep the documented proto meaning, where 0 is no expiry.
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

// parseWaitParam interprets the `wait` query parameter. It accepts a Go duration string
// ("30s", "1m500ms") or a bare number of seconds ("30"). An empty value means "do not
// wait". The result is clamped to [0, maxTelemetryWait]; a negative value is treated as 0.
func parseWaitParam(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}

	wait, err := time.ParseDuration(raw)
	if err != nil {
		seconds, convErr := strconv.ParseFloat(raw, 64)
		if convErr != nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"invalid wait %q: expected a duration such as \"30s\" or a number of seconds", raw)
		}
		wait = time.Duration(seconds * float64(time.Second))
	}

	if wait < 0 {
		return 0, nil
	}
	if wait > maxTelemetryWait {
		wait = maxTelemetryWait
	}
	return wait, nil
}

// findCommandReplies looks up every reply to a command. clientID may be empty, in which
// case every known client is scanned; passing it turns the lookup into a targeted one,
// which is what callers who pushed a client-scoped command should do.
//
// It returns the replies, each tagged with the client that produced it, and how many
// clients were examined so a caller can tell a complete answer from a partial one. Replies
// are ordered by client ID so that repeating the request yields the same order -- the
// underlying iteration is over a sync.Map, whose order is unspecified.
//
// An empty result with a nil error means the command has not been answered yet -- a normal
// state, not a failure, because replies only arrive on a client's next heartbeat.
func findCommandReplies(ctx context.Context, node *Proxy, clientID, commandID string) ([]clientCommandReply, int, error) {
	// Metrics are the largest thing a client reports, so skip them: they are not needed to
	// read replies. This does not make the lookup cheap -- command_replies is encoded into
	// ClientInfo.Reserved regardless of this flag -- which is why the poll interval above
	// is measured in seconds.
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

// waitForCommandReply polls for replies until the timeout elapses or the request is
// canceled. A zero timeout performs exactly one lookup.
//
// When clientID is set the command was aimed at one client, so exactly one answer is
// possible and the first one ends the wait. When it is empty the command may have been
// broadcast, and there is no way to know how many clients will answer -- returning on the
// first would hand back one client's answer as though it were the cluster's. So an
// untargeted wait keeps polling for the whole budget and returns everything accumulated.
//
// An empty result with a nil error means "still pending" and is a normal outcome.
func waitForCommandReply(ctx context.Context, node *Proxy, clientID, commandID string, timeout time.Duration) ([]clientCommandReply, int, error) {
	// The budget has to bound the lookups themselves, not just the gaps between them. A
	// timer racing the poll loop leaves every RPC unbounded, so one hung or retrying
	// MixCoord call blows straight through the wait -- a 50ms request included -- and holds
	// a management HTTP handler for however long the call takes. Putting the deadline on the
	// context makes it cover the first lookup and every retry, and doubles as the loop's
	// exit condition so there is only one thing to get right.
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	targeted := clientID != ""

	replies, known, err := findCommandReplies(ctx, node, clientID, commandID)
	if err != nil {
		return nil, 0, budgetAwareErr(ctx, err)
	}
	if timeout <= 0 || (targeted && len(replies) > 0) {
		return replies, known, nil
	}

	ticker := time.NewTicker(telemetryReplyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Budget spent, or the caller hung up. Either way report what was collected;
			// the command itself is unaffected and "pending" is the honest answer.
			return replies, known, nil
		case <-ticker.C:
			latest, latestKnown, err := findCommandReplies(ctx, node, clientID, commandID)
			if err != nil {
				// Keep what was already collected; a transient lookup failure should not
				// discard answers the caller has effectively already waited for.
				return replies, known, budgetAwareErr(ctx, err)
			}
			replies, known = latest, latestKnown
			if targeted && len(replies) > 0 {
				return replies, known, nil
			}
		}
	}
}

// budgetAwareErr drops a lookup error that is really just the wait budget running out
// mid-RPC. Reporting that as a 500 would turn an expired wait into a failure depending on
// whether the deadline landed between calls or inside one.
func budgetAwareErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// commandReplyPayload renders a reply lookup as the JSON body shared by every endpoint
// that can return one, so callers can parse a single shape regardless of which endpoint
// produced it.
//
// `status` is always present: "done" once at least one client has answered, "pending"
// otherwise. Callers should branch on it rather than on the presence of `reply`, and a
// "pending" response is not an error -- retry, or ask again with a longer `wait`.
//
// `replies` is always an array, one entry per answering client. Re-querying later returns
// everything accumulated so far -- replies are retained per client (the most recent 50).
//
// `observed_clients` is how many clients this lookup examined, not how many the command was
// aimed at, and the two are not the same: the scan covers every cached client regardless of
// the command's target scope, includes clients that have gone inactive or connected after
// the command was pushed, and its membership changes between polls. So `responded` reaching
// `observed_clients` does not mean every recipient answered, and falling short of it does
// not mean any are missing. There is deliberately no field claiming completeness, because
// the server does not record who a broadcast command was delivered to; treat both numbers
// as observations.
//
// `reply` and `client_id` repeat the first entry, which is the whole answer for the common
// case of a command aimed at one client. Anything reading a command that was not
// client-scoped must use `replies`. When nothing has answered yet, `client_id` falls back
// to targetClientID so a caller that pushed to a known client still sees who it is waiting
// on; it is omitted when the command named no target.
func commandReplyPayload(commandID, targetClientID string, replies []clientCommandReply, observedClients int, waited time.Duration) map[string]interface{} {
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
	if waited > 0 {
		body["waited_ms"] = waited.Milliseconds()
	}

	if len(replies) == 0 {
		if targetClientID != "" {
			body["client_id"] = targetClientID
		}
		body["message"] = "Command has not been answered yet. The client replies on its " +
			"next heartbeat; retry, or pass ?wait= to block until it arrives."
		return body
	}

	body["status"] = replyStatusDone
	body["client_id"] = replies[0].ClientID
	body["reply"] = replies[0].Reply
	return body
}
