// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type EventDispatcherSuite struct {
	suite.Suite

	dispatcher *EventDispatcher
}

func (s *EventDispatcherSuite) SetupTest() {
	s.dispatcher = NewEventDispatcher()
}

func (s *EventDispatcherSuite) TestRegister() {
	dispatcher := s.dispatcher

	s.Run("test_register_same_key", func() {
		dispatcher.Register("a", NewHandler("handler_1", func(*Event) {}))
		dispatcher.Register("a", NewHandler("handler_2", func(*Event) {}))

		handlers := dispatcher.Get("a")
		s.ElementsMatch([]string{"handler_1", "handler_2"}, lo.Map(handlers, func(h EventHandler, _ int) string { return h.GetIdentifier() }))
	})

	s.Run("test_register_same_id", func() {
		dispatcher.Register("b", NewHandler("handler_1", func(*Event) {}))
		dispatcher.Register("b", NewHandler("handler_1", func(*Event) {}))

		handlers := dispatcher.Get("b")
		s.ElementsMatch([]string{"handler_1", "handler_1"}, lo.Map(handlers, func(h EventHandler, _ int) string { return h.GetIdentifier() }))
	})
}

func (s *EventDispatcherSuite) TestRegisterForKeyPrefix() {
	dispatcher := s.dispatcher

	s.Run("test_register_same_key", func() {
		dispatcher.RegisterForKeyPrefix("a", NewHandler("handler_1", func(*Event) {}))
		dispatcher.RegisterForKeyPrefix("a", NewHandler("handler_2", func(*Event) {}))

		handlers := dispatcher.Get("a")
		s.ElementsMatch([]string{"handler_1", "handler_2"}, lo.Map(handlers, func(h EventHandler, _ int) string { return h.GetIdentifier() }))
		s.Contains(dispatcher.keyPrefix, "a")
	})

	s.Run("test_register_same_id", func() {
		dispatcher.RegisterForKeyPrefix("b", NewHandler("handler_1", func(*Event) {}))
		dispatcher.RegisterForKeyPrefix("b", NewHandler("handler_1", func(*Event) {}))

		handlers := dispatcher.Get("b")
		s.ElementsMatch([]string{"handler_1", "handler_1"}, lo.Map(handlers, func(h EventHandler, _ int) string { return h.GetIdentifier() }))
		s.Contains(dispatcher.keyPrefix, "b")
	})
}

func (s *EventDispatcherSuite) TestUnregister() {
	dispatcher := s.dispatcher

	s.Run("unregister_non_exist_key", func() {
		s.NotPanics(func() {
			dispatcher.Unregister("non_register", NewHandler("handler_1", func(*Event) {}))
		})
	})

	s.Run("unregister_non_exist_id", func() {
		dispatcher.Register("b", NewHandler("handler_1", func(*Event) {}))

		s.NotPanics(func() {
			dispatcher.Unregister("b", NewHandler("handler_2", func(*Event) {}))
		})

		handlers := dispatcher.Get("b")
		s.ElementsMatch([]string{"handler_1"}, lo.Map(handlers, func(h EventHandler, _ int) string { return h.GetIdentifier() }))
	})

	s.Run("unregister_exist_handler", func() {
		dispatcher.Register("c", NewHandler("handler_1", func(*Event) {}))

		s.NotPanics(func() {
			dispatcher.Unregister("c", NewHandler("handler_1", func(*Event) {}))
		})

		handlers := dispatcher.Get("c")
		s.ElementsMatch([]string{}, lo.Map(handlers, func(h EventHandler, _ int) string { return h.GetIdentifier() }))
	})
}

func (s *EventDispatcherSuite) TestDispatch() {
	dispatcher := s.dispatcher

	s.Run("dispatch_key_event", func() {
		called := atomic.NewBool(false)

		dispatcher.Register("a", NewHandler("handler_1", func(*Event) { called.Store(true) }))

		dispatcher.Dispatch(newEvent("test", "test", "aa", "b"))

		s.False(called.Load())

		dispatcher.Dispatch(newEvent("test", "test", "a", "b"))

		s.True(called.Load())
	})

	s.Run("dispatch_prefix_event", func() {
		called := atomic.NewBool(false)

		dispatcher.RegisterForKeyPrefix("b", NewHandler("handler_1", func(*Event) { called.Store(true) }))

		dispatcher.Dispatch(newEvent("test", "test", "bb", "b"))

		s.True(called.Load())
	})
}

func TestEventDispatcher(t *testing.T) {
	suite.Run(t, new(EventDispatcherSuite))
}
