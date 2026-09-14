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

package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	refreshPublicKey = "function.textembedding.providers.demo.enable"
	refreshSecretKey = "function.textembedding.providers.demo_enable"
	refreshCanary    = "refresh-secret-canary"
)

func newRefreshManager() *Manager {
	mgr := NewManager()
	mgr.RegisterConfigPrefix("function.textembedding.providers.")
	mgr.RegisterSensitivePrefix("function.textembedding.providers.")
	mgr.RegisterNonSensitiveSuffix("function.textembedding.providers.", "enable")
	return mgr
}

func refreshValues(key, value string) map[string]string {
	return map[string]string{key: value, formatKey(key): value}
}

func awaitRefresh[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for configuration refresh barrier/result")
		var zero T
		return zero
	}
}

func assertRefreshSafe(t *testing.T, mgr *Manager) {
	t.Helper()
	for _, projection := range []map[string]string{
		mgr.ProjectConfigs(),
		mgr.ProjectBy(WithPrefix("function")),
		mgr.GetConfigsView(),
	} {
		for key, value := range projection {
			assert.NotContains(t, value, refreshCanary, "projected key %s", key)
		}
	}
	for _, key := range []string{refreshPublicKey, refreshSecretKey, formatKey(refreshPublicKey)} {
		_, value, err := mgr.GetRegisteredConfig(key)
		assert.True(t, mgr.IsSensitive(key), "sensitivity history must survive removal")
		// A removed or not-yet-indexed value now reports absence even when
		// its identity remains sensitive.
		if !errors.Is(err, ErrKeyNotFound) {
			assert.ErrorIs(t, err, ErrKeySensitive)
		}
		assert.Empty(t, value)
	}
}

// The barrier runs after the source publishes its map but before Manager sees
// even the first event. Concurrent management reads can run in this window.
func TestSourceRefreshPublishesPolicyWithValues(t *testing.T) {
	for _, kind := range []string{"file", "etcd"} {
		t.Run(kind, func(t *testing.T) {
			mgr := newRefreshManager()
			t.Cleanup(mgr.Close)
			var source Source
			var update func(map[string]string) error
			if kind == "file" {
				fs := NewFileSource(&FileInfo{})
				source, update = fs, fs.update
			} else {
				es := &EtcdSource{ctx: context.Background(), currentConfigs: make(map[string]string)}
				es.configRefresher = newRefresher(0, nil)
				source, update = es, es.update
			}
			source.SetManager(mgr)
			mgr.sources.Insert(source.GetSourceName(), source)
			source.SetEventHandler(mgr)
			require.NoError(t, update(refreshValues(refreshPublicKey, "true")))
			_, value, err := mgr.GetRegisteredConfig(refreshPublicKey)
			require.NoError(t, err)
			require.Equal(t, "true", value)
			events := 0
			source.SetEventHandler(NewHandler("read-before-event", func(e *Event) {
				events++
				assertRefreshSafe(t, mgr)
				mgr.OnEvent(e)
			}))
			require.NoError(t, update(refreshValues(refreshSecretKey, refreshCanary)))
			require.Positive(t, events)
			assertRefreshSafe(t, mgr)
		})
	}
}

func TestFileReloadPublishesPolicyBeforeEvents(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(filename, []byte(refreshPublicKey+": true\n"), 0o600))
	fs := NewFileSource(&FileInfo{Files: []string{filename}})
	mgr := newRefreshManager()
	t.Cleanup(mgr.Close)
	require.NoError(t, mgr.AddSource(fs))
	fs.SetEventHandler(NewHandler("reload-barrier", func(e *Event) {
		assertRefreshSafe(t, mgr)
		mgr.OnEvent(e)
	}))
	require.NoError(t, os.WriteFile(filename, []byte(refreshSecretKey+": "+refreshCanary+"\n"), 0o600))
	require.NotEmpty(t, mgr.ProjectFileConfigs())
	assertRefreshSafe(t, mgr)
}

// An external getter must not classify the old generation and then read a
// replacement value. Embed a real FileSource and pause at its read boundary.
type pausedFileSource struct {
	*FileSource
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (s *pausedFileSource) GetConfigurationByKey(key string) (string, error) {
	s.once.Do(func() {
		close(s.entered)
		<-s.resume
	})
	return s.FileSource.GetConfigurationByKey(key)
}

func TestRegisteredReadCannotCrossSourceRefresh(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(filename, []byte(refreshPublicKey+": true\n"), 0o600))
	source := &pausedFileSource{
		FileSource: NewFileSource(&FileInfo{Files: []string{filename}}),
		entered:    make(chan struct{}), resume: make(chan struct{}),
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(source.resume) }) })
	mgr := newRefreshManager()
	t.Cleanup(mgr.Close)
	require.NoError(t, mgr.AddSource(source))
	type answer struct {
		value string
		err   error
	}
	result := make(chan answer, 1)
	go func() {
		_, value, err := mgr.GetRegisteredConfig(refreshPublicKey)
		result <- answer{value, err}
	}()
	awaitRefresh(t, source.entered)
	updated := make(chan error, 1)
	go func() { updated <- source.update(refreshValues(refreshSecretKey, refreshCanary)) }()
	// Before the fix, publication completes during the paused read. With a
	// consistent snapshot it waits for the reader, which still sees true.
	updateFinished := false
	select {
	case err := <-updated:
		require.NoError(t, err)
		updateFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	release.Do(func() { close(source.resume) })
	got := awaitRefresh(t, result)
	assert.NotContains(t, got.value, refreshCanary)
	if got.err == nil {
		assert.Equal(t, "true", got.value)
	}
	if !updateFinished {
		require.NoError(t, awaitRefresh(t, updated))
	}
	assertRefreshSafe(t, mgr)
}

func TestGroupOverlayRemovalByAliases(t *testing.T) {
	const key = "public.group.a.b"
	spellings := []string{key, "public/group/a/b", "PUBLIC_GROUP_A_B", "PUBLICGROUPAB"}
	for _, stored := range spellings {
		for _, removed := range spellings {
			for _, operation := range []string{"delete", "reset"} {
				t.Run(stored+"/"+removed+"/"+operation, func(t *testing.T) {
					mgr := NewManager()
					mgr.RegisterConfigPrefix("public.group.")
					mgr.SetMapConfig(stored, "overlay-canary")
					if operation == "delete" {
						mgr.DeleteConfig(removed)
					} else {
						mgr.ResetConfig(removed)
					}
					for _, value := range mgr.GetEffectiveBy(WithPrefix("public")) {
						assert.NotEqual(t, "overlay-canary", value)
					}
					for _, value := range mgr.ProjectConfigs() {
						assert.NotEqual(t, "overlay-canary", value)
					}
					_, value, err := mgr.GetRegisteredConfig(key)
					assert.ErrorIs(t, err, ErrKeyNotFound)
					assert.Empty(t, value)
				})
			}
		}
	}
}

func captureConfigLogs(t *testing.T) *syncBuffer {
	t.Helper()
	logs := &syncBuffer{}
	logger, props, err := mlog.InitLoggerWithWriteSyncer(&mlog.Config{
		Level: "debug", Format: "text", DisableCaller: true,
		DisableTimestamp: true, DisableStacktrace: true,
	}, logs)
	require.NoError(t, err)
	oldLogger, oldLevel := mlog.L(), mlog.GetAtomicLevel()
	mlog.ReplaceGlobals(logger, props)
	t.Cleanup(func() { mlog.ReplaceGlobals(oldLogger, &mlog.ZapProperties{Level: oldLevel}) })
	return logs
}

func TestConfigMutationLogsDoNotExposeRequestNames(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	info := &EtcdInfo{Endpoints: strings.Split(endpoints, ","), KeyPrefix: "test-config-logs-" + t.Name()}
	client, err := newEtcdClient(info)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	source, err := NewEtcdSource(client, info)
	require.NoError(t, err)
	mgr := NewManager()
	mgr.RegisterConfigPrefix("public.group.")
	require.NoError(t, mgr.AddSource(source))
	t.Cleanup(mgr.Close)
	logs := captureConfigLogs(t)
	key := "public.group.request-name-canary"
	require.NoError(t, mgr.AlterConfigsInEtcd(source, map[string]string{key: "request-value-canary"}, nil))
	require.NoError(t, mgr.AlterConfigsInEtcd(source, nil, []string{key, "unknown-delete-canary"}))
	for _, canary := range []string{"request-name-canary", "request-value-canary", "unknown-delete-canary"} {
		assert.NotContains(t, logs.String(), canary)
	}
	assert.Contains(t, logs.String(), "configs atomically altered in etcd")
}

// A new lower-priority spelling must not turn a previously unsegmented,
// sensitive higher-priority value into an exempt leaf. This is a lasting
// policy widening, not only the publication window covered above.
func TestNewSpellingDoesNotExposeExistingUnsegmentedValue(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(filename, []byte("{}\n"), 0o600))
	fs := NewFileSource(&FileInfo{Files: []string{filename}})
	mgr := newRefreshManager()
	t.Cleanup(mgr.Close)
	require.NoError(t, mgr.AddSource(fs))
	require.NoError(t, mgr.AddSource(&mapSource{name: "opaque-higher-priority", configs: map[string]string{
		formatKey(refreshPublicKey): refreshCanary,
	}}))
	_, _, err := mgr.GetRegisteredConfig(refreshPublicKey)
	require.ErrorIs(t, err, ErrKeySensitive)
	require.NoError(t, os.WriteFile(filename, []byte(refreshPublicKey+": true\n"), 0o600))
	_, err = fs.GetConfigurations()
	require.NoError(t, err)
	source, raw, err := mgr.GetConfig(refreshPublicKey)
	require.NoError(t, err)
	require.Equal(t, "opaque-higher-priority", source)
	require.Equal(t, refreshCanary, raw, "source priority and raw reads must remain unchanged")
	assertRefreshSafe(t, mgr)
}

// Each built-in source publishes before dispatching its CREATE events. A
// second source or overlay can introduce a spelling while the first value is
// absent from keySourceMap; that spelling must never endorse the earlier value.
func TestNewSpellingCannotEndorseUnindexedSourceValue(t *testing.T) {
	for _, firstSource := range []string{"file", "etcd"} {
		for _, spellingSource := range []string{"other-source", "overlay"} {
			t.Run(firstSource+"/"+spellingSource, func(t *testing.T) {
				mgr := newRefreshManager()
				t.Cleanup(mgr.Close)
				fs := NewFileSource(&FileInfo{})
				es := &EtcdSource{ctx: context.Background(), currentConfigs: make(map[string]string)}
				es.configRefresher = newRefresher(0, nil)
				for _, source := range []Source{fs, es} {
					source.SetManager(mgr)
					mgr.sources.Insert(source.GetSourceName(), source)
					source.SetEventHandler(mgr)
				}
				var first Source = fs
				publishFirst, publishOther := fs.update, es.update
				if firstSource == "etcd" {
					first, publishFirst, publishOther = es, es.update, fs.update
				}
				entered, resume := make(chan struct{}), make(chan struct{})
				var release sync.Once
				t.Cleanup(func() { release.Do(func() { close(resume) }) })
				first.SetEventHandler(NewHandler("unindexed-publication", func(e *Event) {
					close(entered)
					<-resume
					// Reentrant safe readers must remain legal in source callbacks.
					assertRefreshSafe(t, mgr)
					mgr.OnEvent(e)
				}))
				done := make(chan error, 1)
				go func() {
					done <- publishFirst(map[string]string{formatKey(refreshPublicKey): refreshCanary})
				}()
				awaitRefresh(t, entered)
				_, indexed := mgr.keySourceMap.Get(formatKey(refreshPublicKey))
				require.False(t, indexed, "the first CREATE event has not reached the manager")
				spellingDone := make(chan error, 1)
				go func() {
					if spellingSource == "overlay" {
						mgr.SetMapConfig(refreshPublicKey, "true")
						spellingDone <- nil
					} else {
						spellingDone <- publishOther(refreshValues(refreshPublicKey, "true"))
					}
				}()
				require.NoError(t, awaitRefresh(t, spellingDone))
				assertRefreshSafe(t, mgr)
				release.Do(func() { close(resume) })
				require.NoError(t, awaitRefresh(t, done))
				assertRefreshSafe(t, mgr)
				// Remove the newer spelling/value so source priority exposes the
				// original value in both source-order directions.
				if spellingSource == "overlay" {
					mgr.ResetConfig(refreshPublicKey)
				} else {
					require.NoError(t, publishOther(map[string]string{}))
				}
				source, value, err := mgr.GetConfig(refreshPublicKey)
				require.NoError(t, err)
				require.Equal(t, first.GetSourceName(), source)
				require.Equal(t, refreshCanary, value)
				assertRefreshSafe(t, mgr)
			})
		}
	}
}

// Initial source pulls and runtime setters must retain the same history even
// after a value is removed: a delayed alias/event cannot turn an identity with
// an earlier unsegmented value into a public suffix.
func TestUnsegmentedPublicationHistorySurvivesRemoval(t *testing.T) {
	for _, origin := range []string{"initial-file", "initial-source", "overlay", "map-overlay"} {
		t.Run(origin, func(t *testing.T) {
			mgr := newRefreshManager()
			t.Cleanup(mgr.Close)
			folded := formatKey(refreshPublicKey)
			switch origin {
			case "initial-file":
				filename := filepath.Join(t.TempDir(), "config.yaml")
				require.NoError(t, os.WriteFile(filename, []byte(folded+": "+refreshCanary+"\n"), 0o600))
				fs := NewFileSource(&FileInfo{Files: []string{filename}})
				require.NoError(t, mgr.AddSource(fs))
				require.NoError(t, fs.update(map[string]string{}))
			case "initial-source":
				require.NoError(t, mgr.AddSource(&mapSource{name: "initial-source", configs: map[string]string{folded: refreshCanary}}))
				mgr.OnEvent(&Event{EventSource: "initial-source", EventType: DeleteType, Key: folded})
			case "overlay":
				mgr.SetConfig(folded, refreshCanary)
				mgr.ResetConfig(folded)
			case "map-overlay":
				mgr.SetMapConfig(folded, refreshCanary)
				mgr.ResetConfig(folded)
			}
			mgr.SetMapConfig(refreshPublicKey, refreshCanary)
			assertRefreshSafe(t, mgr)
		})
	}
}

func TestEstablishedSpellingSurvivesSourceAndOverlayOverrides(t *testing.T) {
	t.Setenv("FUNCTION_TEXTEMBEDDING_PROVIDERS_DEMO_ENABLE", "from-env")
	filename := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(filename, []byte(refreshPublicKey+": from-file\n"), 0o600))
	fs := NewFileSource(&FileInfo{Files: []string{filename}})
	mgr := newRefreshManager()
	t.Cleanup(mgr.Close)
	require.NoError(t, mgr.AddSource(fs))
	assertPublic := func(want string) {
		t.Helper()
		for _, key := range []string{refreshPublicKey, formatKey(refreshPublicKey)} {
			_, value, err := mgr.GetRegisteredConfig(key)
			require.NoError(t, err)
			require.Equal(t, want, value)
		}
		require.Equal(t, want, mgr.ProjectConfigs()[refreshPublicKey])
	}
	assertPublic("from-file")
	require.NoError(t, mgr.AddSource(NewEnvSource(formatKey)))
	assertPublic("from-env")
	es := &EtcdSource{ctx: context.Background(), currentConfigs: make(map[string]string)}
	es.configRefresher = newRefresher(0, nil)
	es.SetManager(mgr)
	mgr.sources.Insert(es.GetSourceName(), es)
	es.SetEventHandler(mgr)
	require.NoError(t, es.update(map[string]string{formatKey(refreshPublicKey): "from-etcd"}))
	assertPublic("from-etcd")
	mgr.SetConfig(refreshPublicKey, "from-overlay")
	assertPublic("from-overlay")
	mgr.SetMapConfig(refreshPublicKey, "from-map-overlay")
	assertPublic("from-map-overlay")
	mgr.ResetConfig(refreshPublicKey)
	assertPublic("from-etcd")
	require.NoError(t, es.update(map[string]string{}))
	assertPublic("from-env")
}
