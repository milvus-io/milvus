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
	// for a reply. Replies only ever land on a heartbeat, so polling faster buys nothing.
	telemetryReplyPollInterval = 500 * time.Millisecond
)

// Reply lookup outcomes reported to API callers.
const (
	replyStatusDone    = "done"
	replyStatusPending = "pending"
)

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

// findCommandReply looks up a single command reply. clientID may be empty, in which case
// every known client is scanned; passing it turns the lookup into a targeted one, which is
// what callers who just pushed a command should do.
//
// It returns the reply and the ID of the client that produced it. A nil reply with a nil
// error means the command has not been answered yet -- that is a normal state, not a
// failure, because replies only arrive on the client's next heartbeat.
func findCommandReply(ctx context.Context, node *Proxy, clientID, commandID string) (*CommandReply, string, error) {
	// Metrics are not needed to read replies, and skipping them keeps this cheap enough to
	// poll: command_replies is populated independently of IncludeMetrics.
	resp, err := node.GetClientTelemetry(ctx, &milvuspb.GetClientTelemetryRequest{
		ClientId:       clientID,
		IncludeMetrics: false,
	})
	if err != nil {
		return nil, "", err
	}
	if err := merr.Error(resp.GetStatus()); err != nil {
		return nil, "", err
	}

	for _, client := range ConvertClientTelemetryResponse(resp).Clients {
		for _, reply := range client.CommandReplies {
			if reply.CommandID == commandID {
				return reply, client.ClientID, nil
			}
		}
	}

	return nil, "", nil
}

// waitForCommandReply polls for a command reply until it appears, the timeout elapses, or
// the request is canceled. A zero timeout performs exactly one lookup.
//
// A nil reply with a nil error means "still pending" and is a normal outcome.
func waitForCommandReply(ctx context.Context, node *Proxy, clientID, commandID string, timeout time.Duration) (*CommandReply, string, error) {
	reply, replyClientID, err := findCommandReply(ctx, node, clientID, commandID)
	if err != nil || reply != nil || timeout <= 0 {
		return reply, replyClientID, err
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(telemetryReplyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Caller hung up or the server is shutting down; report as still pending
			// rather than as an error, the command itself is unaffected.
			return nil, "", nil
		case <-ticker.C:
			reply, replyClientID, err = findCommandReply(ctx, node, clientID, commandID)
			if err != nil || reply != nil {
				return reply, replyClientID, err
			}
			if !time.Now().Before(deadline) {
				return nil, "", nil
			}
		}
	}
}

// commandReplyPayload renders a reply lookup as the JSON body shared by every endpoint
// that can return one, so callers can parse a single shape regardless of which endpoint
// produced it.
//
// `status` is always present: "done" when the client has answered, "pending" otherwise.
// Callers should branch on it rather than on the presence of `reply`, and a "pending"
// response is not an error -- retry, or ask again with a longer `wait`.
func commandReplyPayload(commandID, clientID string, reply *CommandReply, waited time.Duration) map[string]interface{} {
	body := map[string]interface{}{
		"command_id": commandID,
		"status":     replyStatusPending,
	}
	if clientID != "" {
		body["client_id"] = clientID
	}
	if waited > 0 {
		body["waited_ms"] = waited.Milliseconds()
	}

	if reply == nil {
		body["message"] = "Command has not been answered yet. The client replies on its " +
			"next heartbeat; retry, or pass ?wait= to block until it arrives."
		return body
	}

	body["status"] = replyStatusDone
	body["reply"] = reply
	return body
}
