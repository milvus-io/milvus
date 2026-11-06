// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"context"
	"math/rand"
	"time"
)

const connectionMaxAgeJitter = 0.1

func (c *Client) acquireConnection() *clientConn {
	c.connectionsMut.RLock()
	defer c.connectionsMut.RUnlock()

	if c.closed {
		return nil
	}
	connection := c.selectConnection()
	if connection == nil {
		return nil
	}
	// Selection and acquisition are atomic with respect to retirement.
	connection.inflight.Add(1)
	return connection
}

func (connection *clientConn) release() {
	if connection.inflight.Add(-1) == 0 && connection.retiring.Load() {
		connection.signalDrained()
	}
}

func (connection *clientConn) retire() {
	connection.retiring.Store(true)
	if connection.inflight.Load() == 0 {
		connection.signalDrained()
	}
}

func (connection *clientConn) signalDrained() {
	connection.drainOnce.Do(func() {
		close(connection.drained)
	})
}

func (connection *clientConn) waitForDrain(ctx context.Context, timeout time.Duration) {
	if connection.inflight.Load() == 0 {
		return
	}

	if timeout <= 0 {
		select {
		case <-connection.drained:
		case <-ctx.Done():
		}
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-connection.drained:
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (c *Client) startConnectionRotation() {
	if c.config.ConnectionMaxAge <= 0 {
		return
	}

	c.connectionsMut.RLock()
	connectionCount := len(c.connections)
	c.connectionsMut.RUnlock()
	for index := 0; index < connectionCount; index++ {
		c.rotationWG.Add(1)
		go c.rotateConnectionLoop(index)
	}
}

func (c *Client) rotateConnectionLoop(index int) {
	defer c.rotationWG.Done()

	delay := c.nextConnectionMaxAge()
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-c.rotationCtx.Done():
			return
		case <-timer.C:
		}

		if err := c.rotateConnection(index); err != nil {
			if c.rotationCtx.Err() != nil {
				return
			}
			delay = c.rotationRetryDelay()
		} else {
			delay = c.nextConnectionMaxAge()
		}
		timer.Reset(delay)
	}
}

func (c *Client) rotateConnection(index int) error {
	ctx, cancel := context.WithTimeout(c.rotationCtx, c.config.getConnectionRotationTimeout())
	defer cancel()

	c.lifecycleMut.RLock()
	replacement, err := c.newConnection(ctx, c.config.getParsedAddress())
	c.lifecycleMut.RUnlock()
	if err != nil {
		return err
	}

	c.connectionsMut.Lock()
	if c.closed || index >= len(c.connections) {
		c.connectionsMut.Unlock()
		_ = replacement.conn.Close()
		return context.Canceled
	}
	old := c.connections[index]
	c.connections[index] = replacement
	old.retire()
	c.connectionsMut.Unlock()

	// Each slot drains synchronously, bounding retained transports to one old
	// connection per slot. Other slots and requests on the replacement continue.
	old.waitForDrain(c.rotationCtx, c.config.ConnectionDrainTimeout)
	_ = old.conn.Close()
	return nil
}

func (c *Client) nextConnectionMaxAge() time.Duration {
	maxAge := c.config.ConnectionMaxAge
	jitter := (rand.Float64()*2 - 1) * connectionMaxAgeJitter
	delay := time.Duration(float64(maxAge) * (1 + jitter))
	if delay <= 0 {
		return time.Nanosecond
	}
	return delay
}

func (c *Client) rotationRetryDelay() time.Duration {
	delay := c.config.ConnectionMaxAge / 10
	if delay < time.Second {
		delay = time.Second
	}
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	return delay
}
