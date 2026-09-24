/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hookutil

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// recordingCipher is a compiled-in cipher that remembers how often it was
// initialized and can refuse. The reload callback re-initializes it from a
// configuration event, so the count is taken under a lock.
type recordingCipher struct {
	testCipher
	initErr error

	mu    sync.Mutex
	inits int
}

func (c *recordingCipher) Init(map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inits++
	return c.initErr
}

func (c *recordingCipher) initCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inits
}

var _ hook.Cipher = (*recordingCipher)(nil)

// installCipher makes this test's binary one with a cipher compiled in, on a
// cipher state no earlier test has touched.
func installCipher(t *testing.T, c hook.Cipher) {
	t.Helper()
	paramtable.Init()
	initCipherOnce = sync.Once{}
	storeCipher(nil)
	ext.ResetForTest()
	t.Cleanup(func() {
		ext.ResetForTest()
		initCipherOnce = sync.Once{}
		storeCipher(nil)
	})
	ext.SetCipher(c)
}

// saveCipherKey writes one of the two cipherPlugin.so* path keys and clears
// it when the test ends. Both default to empty, which is what clearing
// restores; a key with a non-empty default would need its value read first.
func saveCipherKey(t *testing.T, key, value string) {
	t.Helper()
	cp := paramtable.GetCipherParams()
	require.NoError(t, cp.Save(key, value))
	t.Cleanup(func() { _ = cp.Save(key, "") })
}

// installedCipher reads the stored cipher without going through GetCipher,
// whose InitOnceCipher would run initCipher a second time after a test that
// called it directly - and would panic on the error those tests are about.
func installedCipher() hook.Cipher {
	return Cipher.Load().(cipherContainer).cipher
}

func TestInitCipherUsesTheCompiledInCipher(t *testing.T) {
	c := &recordingCipher{}
	installCipher(t, c)
	saveCipherKey(t, paramtable.GetCipherParams().SoPathCpp.Key, "/tmp/some-cipher.so")

	InitOnceCipher()

	assert.Same(t, c, GetCipher())
	assert.True(t, IsClusterEncryptionEnabled(), "a compiled-in cipher with the C++ half configured is encryption on")
	assert.Equal(t, 1, c.initCount(), "initialized once, with the cipherPlugin.* configuration, as a plug-in is")
}

// Two answers for the same keys is a deployment mistake, and it is reported
// rather than silently resolved by start-up order.
func TestInitCipherRefusesACompiledInCipherBesideAPlugin(t *testing.T) {
	installCipher(t, &recordingCipher{})
	saveCipherKey(t, paramtable.GetCipherParams().SoPathGo.Key, "/tmp/some-cipher-go.so")
	saveCipherKey(t, paramtable.GetCipherParams().SoPathCpp.Key, "/tmp/some-cipher.so")

	err := initCipher()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one can")
	assert.Nil(t, installedCipher())
}

// The C++ half is still a file the core loads from cipherPlugin.soPathCpp, so
// that path stays the declaration that encryption is on. Without it the
// compiled-in cipher is left idle, as no plug-in would be loaded.
func TestInitCipherLeavesTheCompiledInCipherIdleWithoutTheCppHalf(t *testing.T) {
	c := &recordingCipher{}
	installCipher(t, c)

	InitOnceCipher()

	assert.Nil(t, GetCipher())
	assert.False(t, IsClusterEncryptionEnabled())
	assert.Equal(t, 0, c.initCount(), "an idle cipher is not initialized: it has no KMS configuration to initialize with")
}

func TestInitCipherReportsACompiledInCipherThatCannotInitialize(t *testing.T) {
	installCipher(t, &recordingCipher{initErr: errors.New("no kms key")})
	saveCipherKey(t, paramtable.GetCipherParams().SoPathCpp.Key, "/tmp/some-cipher.so")

	err := initCipher()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no kms key")
	assert.Contains(t, err.Error(), "compiled-in cipher")
	assert.Nil(t, installedCipher(), "a cipher that cannot initialize is not installed")
}

// With nothing compiled in - a stock binary - nothing changes: no so paths,
// no cipher.
func TestInitCipherWithoutACompiledInCipherIsUnchanged(t *testing.T) {
	installCipher(t, nil)

	InitOnceCipher()
	assert.Nil(t, GetCipher())
}

// A compiled-in cipher is reconfigured exactly as a plug-in is: a
// cipherPlugin.* edit re-initializes it.
func TestReloadCipherConfigReachesTheCompiledInCipher(t *testing.T) {
	c := &recordingCipher{}
	installCipher(t, c)
	saveCipherKey(t, paramtable.GetCipherParams().SoPathCpp.Key, "/tmp/some-cipher.so")
	InitOnceCipher()
	require.Equal(t, 1, c.initCount())

	require.NoError(t, reloadCipherConfig(context.Background(), paramtable.GetCipherParams().DefaultRootKey.Key, "", "aws-kms://new"))
	assert.Equal(t, 2, c.initCount())
}

// And with nothing compiled in, cipherPlugin.soPathGo is still loaded as a
// plug-in: the compiled-in branch must not swallow the path a stock binary
// takes. A file that is not there is the nearest thing to a plug-in a test
// can load.
func TestInitCipherWithoutACompiledInCipherStillLoadsThePlugin(t *testing.T) {
	installCipher(t, nil)
	saveCipherKey(t, paramtable.GetCipherParams().SoPathGo.Key, "/tmp/no-such-cipher-go.so")
	saveCipherKey(t, paramtable.GetCipherParams().SoPathCpp.Key, "/tmp/no-such-cipher.so")

	err := initCipher()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail to open plugin")
	assert.Nil(t, installedCipher())
}
