// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package onnx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	defaultMaxSessions    = 32
	defaultMaxConcurrency = 4
	defaultIdleTTL        = 5 * time.Minute
	maxHashBuffer         = 1 << 20
	maxModelProtoDepth    = 32
)

var (
	errModelTooLarge       = errors.New("onnx: model exceeds configured size")
	errMalformedModelProto = errors.New("onnx: malformed model protobuf")
	errModelProtoTooDeep   = errors.New("onnx: model protobuf nesting exceeds limit")
)

// Config bounds resident sessions, inference calls, and runtime memory.
// Zero values select conservative defaults, except MaxMemoryBytes where zero
// means unlimited (the runtime remains bounded by MaxSessions).
type Config struct {
	MaxSessions    int
	MaxConcurrency int
	MaxMemoryBytes uint64
	IdleTTL        time.Duration
	MaxModelBytes  int64
}

func (c Config) normalized() (Config, error) {
	if c.MaxSessions == 0 {
		c.MaxSessions = defaultMaxSessions
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = defaultMaxConcurrency
	}
	if c.IdleTTL == 0 {
		c.IdleTTL = defaultIdleTTL
	}
	if c.MaxSessions < 1 {
		return Config{}, merr.WrapErrParameterInvalidMsg("onnx: max sessions must be positive")
	}
	if c.MaxConcurrency < 1 {
		return Config{}, merr.WrapErrParameterInvalidMsg("onnx: max concurrency must be positive")
	}
	if c.IdleTTL < 0 {
		return Config{}, merr.WrapErrParameterInvalidMsg("onnx: idle TTL cannot be negative")
	}
	if c.MaxModelBytes < 0 {
		return Config{}, merr.WrapErrParameterInvalidMsg("onnx: max model bytes cannot be negative")
	}
	return c, nil
}

type managerEntry struct {
	key       string
	path      string
	session   Session
	memory    uint64
	refs      atomic.Int64
	closing   atomic.Bool
	closed    atomic.Bool
	closeMu   sync.Mutex
	closeDone chan struct{}
	timer     *time.Timer
	timerID   uint64
	manager   *Manager
}

// Manager owns loaded CPU sessions and their lifecycle. A session remains
// alive while a Lease is held; idle sessions are evicted after IdleTTL.
type Manager struct {
	backend        Backend
	backendName    string
	backendVersion string
	config         Config
	sem            *semaphore.Weighted

	mu              sync.Mutex
	entries         map[string]*managerEntry
	memBytes        uint64
	pendingSessions int
	closed          bool
	loadMu          chan struct{}
}

// NewManager validates the runtime policy and creates an empty session cache.
func NewManager(backend Backend, config Config) (*Manager, error) {
	if backend == nil {
		return nil, merr.WrapErrServiceInternalMsg("onnx: backend is nil")
	}
	backendName, backendVersion := backend.Name(), backend.Version()
	if backendName == "" || backendVersion == "" {
		return nil, merr.WrapErrServiceInternalMsg("onnx: backend name and runtime version are required")
	}
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &Manager{
		backend:        backend,
		backendName:    backendName,
		backendVersion: backendVersion,
		config:         config,
		sem:            semaphore.NewWeighted(int64(config.MaxConcurrency)),
		entries:        make(map[string]*managerEntry),
		loadMu:         newLoadGate(),
	}, nil
}

func newLoadGate() chan struct{} {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return gate
}

// Acquire returns a lease for the content-addressed model at modelPath.
// Loads are serialized, so concurrent callers never create duplicate runtime
// sessions for the same model/runtime version.
func (m *Manager) Acquire(ctx context.Context, modelPath string) (*Lease, error) {
	if m == nil {
		return nil, merr.WrapErrServiceInternalMsg("onnx: manager is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "onnx: acquire canceled")
	}
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, merr.WrapErrServiceNotReadyMsg("onnx: manager is closed")
	}
	sourcePath, key, digest, err := m.modelKey(ctx, modelPath)
	if err != nil {
		return nil, err
	}
	if lease, ok := m.tryAcquire(key); ok {
		return lease, nil
	}

	return m.load(ctx, sourcePath, key, digest)
}

func (m *Manager) load(ctx context.Context, sourcePath, key, digest string) (*Lease, error) {
	// Serializing loads keeps the configured resident-memory limit meaningful
	// for backends that cannot estimate their native allocation before loading.
	select {
	case <-ctx.Done():
		return nil, merr.Wrap(ctx.Err(), "onnx: model load canceled")
	case <-m.loadMu:
	}
	defer func() { m.loadMu <- struct{}{} }()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, merr.WrapErrServiceNotReadyMsg("onnx: manager is closed")
	}
	if existing := m.entries[key]; existing != nil && !existing.closing.Load() {
		existing.refs.Add(1)
		m.mu.Unlock()
		return &Lease{entry: existing}, nil
	}
	var evicted []*managerEntry
	if m.sessionCountLocked() >= m.config.MaxSessions && m.pendingSessions == 0 {
		evicted = m.evictIdleLocked(1)
	}
	m.mu.Unlock()
	startCloseEntries(evicted)
	if err := waitForClose(ctx, evicted); err != nil {
		return nil, merr.Wrap(err, "onnx: evict idle sessions")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, merr.WrapErrServiceNotReadyMsg("onnx: manager is closed")
	}
	if existing := m.entries[key]; existing != nil && !existing.closing.Load() {
		existing.refs.Add(1)
		m.mu.Unlock()
		return &Lease{entry: existing}, nil
	}
	if m.sessionCountLocked() >= m.config.MaxSessions {
		m.mu.Unlock()
		return nil, merr.Wrapf(merr.ErrServiceResourceInsufficient, "onnx: maximum resident sessions (%d) reached", m.config.MaxSessions)
	}
	m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "onnx: model load canceled")
	}
	snapshotPath, snapshotDigest, err := snapshotModel(ctx, sourcePath, m.config.MaxModelBytes)
	if err != nil {
		if ctx.Err() != nil {
			return nil, merr.Wrap(ctx.Err(), "onnx: snapshot model")
		}
		if errors.Is(err, errModelTooLarge) {
			return nil, merr.Wrapf(merr.ErrServiceMemoryLimitExceeded, "onnx: model %q exceeds the configured model size", sourcePath)
		}
		return nil, merr.WrapErrIoFailedErr(err, "onnx: snapshot model %q", sourcePath)
	}
	defer os.Remove(snapshotPath)
	if snapshotDigest != digest {
		return nil, merr.WrapErrServiceUnavailableMsg("onnx: model changed while loading %q", sourcePath)
	}
	external, err := modelHasExternalData(ctx, snapshotPath)
	if err != nil {
		if ctx.Err() != nil {
			return nil, merr.Wrap(ctx.Err(), "onnx: inspect model")
		}
		if errors.Is(err, errModelProtoTooDeep) {
			return nil, merr.WrapErrParameterInvalidMsg("onnx: model protobuf nesting exceeds limit")
		}
		return nil, merr.WrapErrIoFailedErr(err, "onnx: inspect model %q", sourcePath)
	}
	if external {
		return nil, merr.WrapErrParameterInvalidMsg("onnx: models with external tensor data are not supported")
	}
	session, err := m.backend.Load(ctx, snapshotPath)
	if err != nil {
		if session != nil {
			_ = session.Close()
		}
		return nil, merr.Wrap(err, "onnx: load model")
	}
	if session == nil {
		return nil, merr.WrapErrServiceInternalMsg("onnx: backend returned a nil session")
	}
	memory := session.MemoryBytes()
	loadedDigest, err := hashFile(ctx, snapshotPath, m.config.MaxModelBytes)
	if err != nil {
		if ctx.Err() != nil {
			discardSessionAsync(m, session, memory, sourcePath)
			return nil, merr.Wrap(ctx.Err(), "onnx: verify model after load")
		}
		_ = session.Close()
		if errors.Is(err, errModelTooLarge) {
			return nil, merr.Wrapf(merr.ErrServiceMemoryLimitExceeded, "onnx: model %q grew beyond the configured model size", sourcePath)
		}
		return nil, merr.WrapErrIoFailedErr(err, "onnx: verify model after load %q", sourcePath)
	}
	if loadedDigest != digest {
		_ = session.Close()
		return nil, merr.WrapErrServiceUnavailableMsg("onnx: model changed while loading %q", sourcePath)
	}
	if err := validateSessionSpecs(session); err != nil {
		_ = session.Close()
		return nil, err
	}
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			_ = session.Close()
			return nil, merr.WrapErrServiceNotReadyMsg("onnx: manager is closed")
		}
		if m.config.MaxMemoryBytes == 0 || (m.memBytes <= m.config.MaxMemoryBytes && memory <= m.config.MaxMemoryBytes-m.memBytes) {
			break
		}
		evicted := m.evictIdleForMemoryLocked(memory)
		available := uint64(0)
		if m.memBytes < m.config.MaxMemoryBytes {
			available = m.config.MaxMemoryBytes - m.memBytes
		}
		m.mu.Unlock()
		if len(evicted) == 0 {
			_ = session.Close()
			return nil, merr.Wrapf(merr.ErrServiceMemoryLimitExceeded, "onnx: resident session memory limit exceeded (%d > %d bytes)", memory, available)
		}
		startCloseEntries(evicted)
		if err := waitForClose(ctx, evicted); err != nil {
			discardSessionAsync(m, session, memory, sourcePath)
			return nil, merr.Wrap(err, "onnx: evict idle sessions for memory")
		}
	}
	if existing := m.entries[key]; existing != nil {
		existing.refs.Add(1)
		m.mu.Unlock()
		_ = session.Close()
		return &Lease{entry: existing}, nil
	}
	entry := &managerEntry{key: key, path: sourcePath, session: session, memory: memory, manager: m, closeDone: make(chan struct{})}
	entry.refs.Store(1)
	m.entries[key] = entry
	m.memBytes += memory
	m.mu.Unlock()
	return &Lease{entry: entry}, nil
}

func (m *Manager) modelKey(ctx context.Context, modelPath string) (string, string, string, error) {
	if modelPath == "" {
		return "", "", "", merr.WrapErrParameterInvalidMsg("onnx: model path is empty")
	}
	if strings.IndexByte(modelPath, 0) >= 0 {
		return "", "", "", merr.WrapErrParameterInvalidMsg("onnx: model path contains NUL")
	}
	abs, err := filepath.Abs(modelPath)
	if err != nil {
		return "", "", "", merr.WrapErrIoFailedErr(err, "onnx: resolve model path")
	}
	info, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", "", "", merr.WrapErrParameterInvalidErr(err, "onnx: stat model %q failed", abs)
		}
		return "", "", "", merr.WrapErrIoFailedErr(err, "onnx: stat model %q failed", abs)
	}
	if !info.Mode().IsRegular() {
		return "", "", "", merr.WrapErrParameterInvalidMsg("onnx: model path %q is not a regular file", abs)
	}
	if m.config.MaxModelBytes > 0 && info.Size() > m.config.MaxModelBytes {
		return "", "", "", merr.Wrapf(merr.ErrServiceMemoryLimitExceeded, "onnx: model %q exceeds the configured model size (%d > %d bytes)", abs, info.Size(), m.config.MaxModelBytes)
	}
	digest, err := hashFile(ctx, abs, m.config.MaxModelBytes)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", "", merr.Wrap(ctx.Err(), "onnx: hash model")
		}
		if errors.Is(err, errModelTooLarge) {
			return "", "", "", merr.Wrapf(merr.ErrServiceMemoryLimitExceeded, "onnx: model %q exceeds the configured model size", abs)
		}
		return "", "", "", merr.WrapErrIoFailedErr(err, "onnx: hash model %q", abs)
	}
	external, err := modelHasExternalData(ctx, abs)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", "", merr.Wrap(ctx.Err(), "onnx: inspect model")
		}
		if errors.Is(err, errModelProtoTooDeep) {
			return "", "", "", merr.WrapErrParameterInvalidMsg("onnx: model protobuf nesting exceeds limit")
		}
		return "", "", "", merr.WrapErrIoFailedErr(err, "onnx: inspect model %q", abs)
	}
	if external {
		return "", "", "", merr.WrapErrParameterInvalidMsg("onnx: models with external tensor data are not supported")
	}
	key := m.backendName + ":" + m.backendVersion + ":" + digest
	return abs, key, digest, nil
}

func snapshotModel(ctx context.Context, path string, maxBytes int64) (string, string, error) {
	source, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer source.Close()
	snapshot, err := os.CreateTemp("", ".milvus-onnx-*"+filepath.Ext(path))
	if err != nil {
		return "", "", err
	}
	snapshotPath := snapshot.Name()
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(snapshotPath)
		}
	}()
	hash := sha256.New()
	buffer := make([]byte, maxHashBuffer)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			_ = snapshot.Close()
			return "", "", err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			total += int64(read)
			if maxBytes > 0 && total > maxBytes {
				_ = snapshot.Close()
				return "", "", errModelTooLarge
			}
			if _, err := hash.Write(buffer[:read]); err != nil {
				_ = snapshot.Close()
				return "", "", err
			}
			if _, err := snapshot.Write(buffer[:read]); err != nil {
				_ = snapshot.Close()
				return "", "", err
			}
		}
		if readErr == io.EOF {
			if err := snapshot.Close(); err != nil {
				return "", "", err
			}
			remove = false
			return snapshotPath, hex.EncodeToString(hash.Sum(nil)), nil
		}
		if readErr != nil {
			_ = snapshot.Close()
			return "", "", readErr
		}
	}
}

func hashFile(ctx context.Context, path string, maxBytes int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	buffer := make([]byte, maxHashBuffer)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		read, readErr := file.Read(buffer)
		if read > 0 {
			total += int64(read)
			if maxBytes > 0 && total > maxBytes {
				return "", errModelTooLarge
			}
			if _, err := hash.Write(buffer[:read]); err != nil {
				return "", err
			}
		}
		if readErr == io.EOF {
			return hex.EncodeToString(hash.Sum(nil)), nil
		}
		if readErr != nil {
			return "", readErr
		}
	}
}

type modelProtoKind uint8

const (
	modelProtoModel modelProtoKind = iota
	modelProtoGraph
	modelProtoNode
	modelProtoAttribute
	modelProtoTensor
	modelProtoSparseTensor
	modelProtoFunction
	modelProtoTrainingInfo
)

// modelHasExternalData follows ONNX's message nesting without loading tensor
// payloads into memory. Malformed protobuf is left to the backend loader.
func modelHasExternalData(ctx context.Context, path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	found, err := scanModelProto(ctx, file, 0, info.Size(), modelProtoModel, 0)
	if errors.Is(err, errMalformedModelProto) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return found, nil
}

func scanModelProto(ctx context.Context, file *os.File, offset, length int64, kind modelProtoKind, depth int) (bool, error) {
	if depth > maxModelProtoDepth {
		return false, errModelProtoTooDeep
	}
	if length < 0 || offset < 0 || offset > int64(^uint64(0)>>1)-length {
		return false, errMalformedModelProto
	}
	end := offset + length
	pos := offset
	for pos < end {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		tag, size, err := readModelVarint(file, pos, end)
		if err != nil {
			return false, err
		}
		pos += size
		field := int(tag >> 3)
		wire := tag & 7
		switch wire {
		case 0:
			value, size, err := readModelVarint(file, pos, end)
			if err != nil {
				return false, err
			}
			pos += size
			if kind == modelProtoTensor && field == 14 && value == 1 {
				return true, nil
			}
		case 1:
			if end-pos < 8 {
				return false, errMalformedModelProto
			}
			pos += 8
		case 2:
			length, size, err := readModelVarint(file, pos, end)
			if err != nil || length > uint64(end-pos-size) {
				return false, errMalformedModelProto
			}
			pos += size
			childStart := pos
			pos += int64(length)
			if kind == modelProtoTensor && field == 13 {
				return true, nil
			}
			childKind, ok := modelProtoChild(kind, field)
			if !ok {
				continue
			}
			found, err := scanModelProto(ctx, file, childStart, int64(length), childKind, depth+1)
			if err != nil || found {
				return found, err
			}
		case 5:
			if end-pos < 4 {
				return false, errMalformedModelProto
			}
			pos += 4
		default:
			return false, errMalformedModelProto
		}
	}
	if pos != end {
		return false, errMalformedModelProto
	}
	return false, nil
}

func modelProtoChild(kind modelProtoKind, field int) (modelProtoKind, bool) {
	switch kind {
	case modelProtoModel:
		switch field {
		case 7:
			return modelProtoGraph, true
		case 20:
			return modelProtoTrainingInfo, true
		case 25:
			return modelProtoFunction, true
		}
	case modelProtoGraph:
		switch field {
		case 1:
			return modelProtoNode, true
		case 5:
			return modelProtoTensor, true
		case 15:
			return modelProtoSparseTensor, true
		}
	case modelProtoNode:
		if field == 5 {
			return modelProtoAttribute, true
		}
	case modelProtoAttribute:
		switch field {
		case 5, 10:
			return modelProtoTensor, true
		case 6, 11:
			return modelProtoGraph, true
		case 22, 23:
			return modelProtoSparseTensor, true
		}
	case modelProtoSparseTensor:
		if field == 1 || field == 2 {
			return modelProtoTensor, true
		}
	case modelProtoFunction:
		switch field {
		case 7:
			return modelProtoNode, true
		case 11:
			return modelProtoAttribute, true
		}
	case modelProtoTrainingInfo:
		if field == 1 || field == 2 {
			return modelProtoGraph, true
		}
	}
	return 0, false
}

func readModelVarint(file *os.File, offset, end int64) (uint64, int64, error) {
	var value uint64
	var bytes [10]byte
	want := end - offset
	if want > int64(len(bytes)) {
		want = int64(len(bytes))
	}
	read, err := file.ReadAt(bytes[:want], offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, 0, err
	}
	for i := 0; i < read; i++ {
		value |= uint64(bytes[i]&0x7f) << (7 * i)
		if bytes[i]&0x80 == 0 {
			return value, int64(i + 1), nil
		}
	}
	return 0, 0, errMalformedModelProto
}

func (m *Manager) tryAcquire(key string) (*Lease, bool) {
	m.mu.Lock()
	entry := m.entries[key]
	m.mu.Unlock()
	return m.acquireEntry(entry)
}

func (m *Manager) acquireEntry(entry *managerEntry) (*Lease, bool) {
	if entry == nil || entry.manager != m {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.entries[entry.key] != entry || entry.closing.Load() {
		return nil, false
	}
	entry.refs.Add(1)
	return &Lease{entry: entry}, true
}

// Lease keeps one model session resident until Release. Release is idempotent.
type Lease struct {
	entry    *managerEntry
	released atomic.Bool
}

func (l *Lease) Session() Session {
	if l == nil || l.entry == nil || l.released.Load() {
		return nil
	}
	return l.entry.session
}

// Identity is the content and runtime version identity used by the cache.
// It is stable for the lifetime of a lease.
func (l *Lease) Identity() string {
	if l == nil || l.entry == nil {
		return ""
	}
	return l.entry.key
}

// Run executes one inference under the manager's concurrency bound.
func (l *Lease) Run(ctx context.Context, inputs []Tensor) ([]Tensor, error) {
	if l == nil || l.entry == nil || l.released.Load() {
		return nil, merr.WrapErrServiceInternalMsg("onnx: lease is released")
	}
	if err := l.entry.manager.sem.Acquire(ctx, 1); err != nil {
		return nil, merr.Wrap(err, "onnx: inference admission")
	}
	defer l.entry.manager.sem.Release(1)
	outputs, err := l.entry.session.Run(ctx, inputs)
	if err != nil {
		return nil, merr.Wrap(err, "onnx: inference failed")
	}
	return outputs, nil
}

func (l *Lease) Release() {
	if l == nil || l.entry == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	entry := l.entry
	refs := entry.refs.Add(-1)
	if refs == 0 && entry.closing.Load() {
		entry.close()
		return
	}
	if refs == 0 {
		entry.manager.scheduleIdleClose(entry)
	}
}

func (m *Manager) scheduleIdleClose(entry *managerEntry) {
	if m.config.IdleTTL == 0 {
		m.removeAndClose(entry)
		return
	}
	m.mu.Lock()
	if m.entries[entry.key] != entry || entry.refs.Load() != 0 || m.closed {
		m.mu.Unlock()
		return
	}
	if entry.timer != nil {
		entry.timer.Stop()
	}
	entry.timerID++
	timerID := entry.timerID
	entry.timer = time.AfterFunc(m.config.IdleTTL, func() {
		m.removeAndCloseTimer(entry, timerID)
	})
	m.mu.Unlock()
}

func (m *Manager) removeAndClose(entry *managerEntry) {
	m.removeAndCloseTimer(entry, 0)
}

func (m *Manager) removeAndCloseTimer(entry *managerEntry, timerID uint64) {
	m.mu.Lock()
	if (timerID != 0 && entry.timerID != timerID) || m.entries[entry.key] != entry || entry.refs.Load() != 0 {
		m.mu.Unlock()
		return
	}
	delete(m.entries, entry.key)
	entry.timerID++
	entry.timer = nil
	m.markClosingLocked(entry)
	m.mu.Unlock()
	entry.close()
}

func (m *Manager) evictIdleLocked(want int) []*managerEntry {
	if want <= 0 {
		return nil
	}
	evicted := make([]*managerEntry, 0, want)
	for key, entry := range m.entries {
		if len(evicted) >= want || entry.refs.Load() != 0 {
			continue
		}
		delete(m.entries, key)
		entry.timerID++
		if entry.timer != nil {
			entry.timer.Stop()
		}
		entry.timer = nil
		m.markClosingLocked(entry)
		evicted = append(evicted, entry)
	}
	return evicted
}

func (m *Manager) evictIdleForMemoryLocked(incoming uint64) []*managerEntry {
	if m.config.MaxMemoryBytes == 0 ||
		(m.memBytes <= m.config.MaxMemoryBytes && incoming <= m.config.MaxMemoryBytes-m.memBytes) {
		return nil
	}
	return m.evictIdleLocked(len(m.entries))
}

func startCloseEntries(entries []*managerEntry) {
	if len(entries) == 0 {
		return
	}
	go func() {
		for _, entry := range entries {
			entry.close()
		}
	}()
}

func discardSessionAsync(m *Manager, session Session, memory uint64, path string) {
	entry := &managerEntry{
		path:      path,
		session:   session,
		memory:    memory,
		manager:   m,
		closeDone: make(chan struct{}),
	}
	m.mu.Lock()
	m.pendingSessions++
	m.memBytes += memory
	m.mu.Unlock()
	startCloseEntries([]*managerEntry{entry})
}

func waitForClose(ctx context.Context, entries []*managerEntry) error {
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-entry.closeDone:
		}
	}
	return nil
}

func (m *Manager) sessionCountLocked() int {
	return len(m.entries) + m.pendingSessions
}

func (m *Manager) markClosingLocked(entry *managerEntry) {
	if entry.closing.CompareAndSwap(false, true) {
		m.pendingSessions++
	}
}

func (m *Manager) finishClose(entry *managerEntry) {
	m.mu.Lock()
	if m.pendingSessions > 0 {
		m.pendingSessions--
	}
	if m.memBytes >= entry.memory {
		m.memBytes -= entry.memory
	} else {
		m.memBytes = 0
	}
	m.mu.Unlock()
}

func (e *managerEntry) close() {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	defer close(e.closeDone)
	if e.session != nil {
		if err := e.session.Close(); err != nil {
			mlog.Warn(context.TODO(), "close onnx session failed", mlog.String("model", e.path), mlog.Err(err))
		}
	}
	e.manager.finishClose(e)
}

// Close evicts idle sessions immediately and defers active session destruction
// until their leases are released.
func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	entries := make([]*managerEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		m.markClosingLocked(entry)
		entries = append(entries, entry)
	}
	m.entries = make(map[string]*managerEntry)
	m.mu.Unlock()
	for _, entry := range entries {
		if entry.refs.Load() == 0 {
			entry.close()
		}
	}
}

// SessionCount reports resident sessions. A session being loaded is excluded.
func (m *Manager) SessionCount() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// MemoryBytes reports native memory charged to resident and pending-close sessions.
func (m *Manager) MemoryBytes() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.memBytes
}
