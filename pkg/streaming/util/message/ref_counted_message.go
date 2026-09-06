package message

import (
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

type refCountedImmutableMessageCore struct {
	mu sync.Mutex

	message       ImmutableMessage
	refCount      int64
	ownerReleased bool
	finalized     bool
	finalizer     func()

	exclusiveCallback           func()
	exclusiveCallbackRegistered bool

	// poisoned is a message-level mark set when a consumer can no longer
	// process this message (e.g. the owning segment failed unrecoverably). It
	// is shared by every handle to the same message and read lock-free by
	// consumers, so it is an atomic rather than guarded by mu.
	poisoned atomic.Bool
}

func (c *refCountedImmutableMessageCore) markPoisoned() {
	c.poisoned.Store(true)
}

func (c *refCountedImmutableMessageCore) isPoisoned() bool {
	return c.poisoned.Load()
}

// NewOwnedImmutableMessage takes ownership of msg and creates its unique root
// reference.
func NewOwnedImmutableMessage(
	msg ImmutableMessage,
	finalizer func(),
) OwnedImmutableMessage {
	if msg == nil {
		panic("ref-counted immutable message is nil")
	}
	core := &refCountedImmutableMessageCore{
		message:   msg,
		refCount:  1,
		finalizer: finalizer,
	}
	return &ownedImmutableMessage{core: core}
}

func (c *refCountedImmutableMessageCore) loadMessage() ImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized || c.message == nil {
		panic("ref-counted immutable message accessed after finalization")
	}
	return c.message
}

func (c *refCountedImmutableMessageCore) clone() RetainedImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized || c.message == nil {
		panic("ref-counted immutable message cloned after finalization")
	}
	c.refCount++
	handle := &retainedImmutableMessage{}
	handle.core.Store(c)
	return handle
}

func (c *refCountedImmutableMessageCore) ownerClone() RetainedImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ownerReleased {
		panic("ref-counted immutable message owner cloned after release")
	}
	c.refCount++
	handle := &retainedImmutableMessage{}
	handle.core.Store(c)
	return handle
}

func (c *refCountedImmutableMessageCore) releaseOwner() {
	c.mu.Lock()
	if c.ownerReleased {
		c.mu.Unlock()
		return
	}
	c.ownerReleased = true
	exclusiveCallback, finalizer, finalized := c.releaseLocked()
	c.mu.Unlock()
	c.invokeExclusiveCallback(exclusiveCallback)
	c.finishFinalization(finalizer, finalized)
}

func (c *refCountedImmutableMessageCore) release() {
	c.mu.Lock()
	exclusiveCallback, finalizer, finalized := c.releaseLocked()
	c.mu.Unlock()
	c.invokeExclusiveCallback(exclusiveCallback)
	c.finishFinalization(finalizer, finalized)
}

func (c *refCountedImmutableMessageCore) releaseLocked() (func(), func(), bool) {
	if c.refCount <= 0 {
		panic("ref-counted immutable message reference count underflow")
	}
	c.refCount--
	var exclusiveCallback func()
	if c.refCount == 1 && !c.ownerReleased && c.exclusiveCallback != nil {
		exclusiveCallback = c.exclusiveCallback
		c.exclusiveCallback = nil
	}
	if c.refCount != 0 || c.finalized {
		return exclusiveCallback, nil, false
	}
	c.finalized = true
	return exclusiveCallback, c.finalizer, true
}

func (c *refCountedImmutableMessageCore) registerExclusiveCallback(callback func()) {
	if callback == nil {
		panic("ref-counted immutable message exclusive callback is nil")
	}
	c.mu.Lock()
	if c.ownerReleased {
		c.mu.Unlock()
		panic("ref-counted immutable message owner accessed after release")
	}
	if c.exclusiveCallbackRegistered {
		c.mu.Unlock()
		panic("ref-counted immutable message exclusive callback already registered")
	}
	c.exclusiveCallbackRegistered = true
	if c.refCount == 1 {
		c.mu.Unlock()
		callback()
		return
	}
	c.exclusiveCallback = callback
	c.mu.Unlock()
}

func (c *refCountedImmutableMessageCore) invokeExclusiveCallback(callback func()) {
	if callback != nil {
		callback()
	}
}

func (c *refCountedImmutableMessageCore) finishFinalization(finalizer func(), finalized bool) {
	if !finalized {
		return
	}
	defer func() {
		c.mu.Lock()
		c.message = nil
		c.finalizer = nil
		c.mu.Unlock()
	}()
	if finalizer != nil {
		finalizer()
	}
}

type ownedImmutableMessage struct {
	core *refCountedImmutableMessageCore
}

func (m *ownedImmutableMessage) Message() ImmutableMessage {
	if m.core == nil {
		panic("ref-counted immutable message owner accessed after release")
	}
	return m.core.loadMessage()
}

func (m *ownedImmutableMessage) Clone() RetainedImmutableMessage {
	if m.core == nil {
		panic("ref-counted immutable message owner cloned after release")
	}
	return m.core.ownerClone()
}

func (m *ownedImmutableMessage) RegisterExclusiveCallback(callback func()) {
	if m.core == nil {
		panic("ref-counted immutable message owner accessed after release")
	}
	m.core.registerExclusiveCallback(callback)
}

func (m *ownedImmutableMessage) Release() {
	if m.core != nil {
		m.core.releaseOwner()
		m.core = nil
	}
}

type retainedImmutableMessage struct {
	core atomic.Pointer[refCountedImmutableMessageCore]
}

func (m *retainedImmutableMessage) Message() ImmutableMessage {
	return m.loadCore().loadMessage()
}

func (m *retainedImmutableMessage) Clone() RetainedImmutableMessage {
	return m.loadCore().clone()
}

func (m *retainedImmutableMessage) Release() {
	if core := m.core.Swap(nil); core != nil {
		core.release()
	}
}

func (m *retainedImmutableMessage) PoisonedRelease() {
	if core := m.core.Swap(nil); core != nil {
		core.markPoisoned()
		core.release()
	}
}

func (m *retainedImmutableMessage) IntoPoisoned() {
	m.loadCore().markPoisoned()
}

func (m *retainedImmutableMessage) IsPoisoned() bool {
	return m.loadCore().isPoisoned()
}

func (*retainedImmutableMessage) retainedImmutableMessage() {}

func (m *retainedImmutableMessage) loadCore() *refCountedImmutableMessageCore {
	core := m.core.Load()
	if core == nil {
		panic("retained immutable message accessed after release")
	}
	return core
}

type specializedOwnedImmutableMessage[H proto.Message, B proto.Message] struct {
	message SpecializedImmutableMessage[H, B]
	owner   OwnedImmutableMessage
}

func (m *specializedOwnedImmutableMessage[H, B]) Message() SpecializedImmutableMessage[H, B] {
	_ = m.owner.Message()
	return m.message
}

func (m *specializedOwnedImmutableMessage[H, B]) Clone() SpecializedRetainedImmutableMessage[H, B] {
	return &specializedRetainedImmutableMessage[H, B]{
		message:  m.message,
		retained: m.owner.Clone(),
	}
}

func (m *specializedOwnedImmutableMessage[H, B]) CloneHandle() RetainedImmutableMessage {
	return m.owner.Clone()
}

func (m *specializedOwnedImmutableMessage[H, B]) Untyped() OwnedImmutableMessage {
	return m.owner
}

type specializedRetainedImmutableMessage[H proto.Message, B proto.Message] struct {
	message  SpecializedImmutableMessage[H, B]
	retained RetainedImmutableMessage
}

func (m *specializedRetainedImmutableMessage[H, B]) Message() SpecializedImmutableMessage[H, B] {
	_ = m.retained.Message()
	return m.message
}

func (m *specializedRetainedImmutableMessage[H, B]) Clone() SpecializedRetainedImmutableMessage[H, B] {
	return &specializedRetainedImmutableMessage[H, B]{
		message:  m.message,
		retained: m.retained.Clone(),
	}
}

func (m *specializedRetainedImmutableMessage[H, B]) CloneHandle() RetainedImmutableMessage {
	return m.retained.Clone()
}

func (m *specializedRetainedImmutableMessage[H, B]) Release() {
	if m.retained != nil {
		m.retained.Release()
		m.retained = nil
		m.message = nil
	}
}

func (m *specializedRetainedImmutableMessage[H, B]) PoisonedRelease() {
	if m.retained != nil {
		m.retained.PoisonedRelease()
		m.retained = nil
		m.message = nil
	}
}

func (m *specializedRetainedImmutableMessage[H, B]) IntoPoisoned() {
	m.retained.IntoPoisoned()
}

func (m *specializedRetainedImmutableMessage[H, B]) IsPoisoned() bool {
	return m.retained.IsPoisoned()
}

type ownedImmutableTxnMessage struct {
	message ImmutableTxnMessage
	owner   OwnedImmutableMessage
}

func (m *ownedImmutableTxnMessage) Message() ImmutableTxnMessage {
	_ = m.owner.Message()
	return m.message
}

func (m *ownedImmutableTxnMessage) Clone() RetainedImmutableTxnMessage {
	return &retainedImmutableTxnMessage{
		message:  m.message,
		retained: m.owner.Clone(),
	}
}

func (m *ownedImmutableTxnMessage) CloneHandle() RetainedImmutableMessage {
	return m.owner.Clone()
}

func (m *ownedImmutableTxnMessage) Untyped() OwnedImmutableMessage {
	return m.owner
}

type retainedImmutableTxnMessage struct {
	message  ImmutableTxnMessage
	retained RetainedImmutableMessage
}

func (m *retainedImmutableTxnMessage) Message() ImmutableTxnMessage {
	_ = m.retained.Message()
	return m.message
}

func (m *retainedImmutableTxnMessage) Clone() RetainedImmutableTxnMessage {
	return &retainedImmutableTxnMessage{
		message:  m.message,
		retained: m.retained.Clone(),
	}
}

func (m *retainedImmutableTxnMessage) CloneHandle() RetainedImmutableMessage {
	return m.retained.Clone()
}

func (m *retainedImmutableTxnMessage) Release() {
	if m.retained != nil {
		m.retained.Release()
		m.retained = nil
		m.message = nil
	}
}

func (m *retainedImmutableTxnMessage) PoisonedRelease() {
	if m.retained != nil {
		m.retained.PoisonedRelease()
		m.retained = nil
		m.message = nil
	}
}

func (m *retainedImmutableTxnMessage) IntoPoisoned() {
	m.retained.IntoPoisoned()
}

func (m *retainedImmutableTxnMessage) IsPoisoned() bool {
	return m.retained.IsPoisoned()
}

// MustAsSpecializedOwnedImmutableMessage converts the message protected by
// owner to a typed immutable message and binds both values in one owned view.
func MustAsSpecializedOwnedImmutableMessage[H proto.Message, B proto.Message](
	owner OwnedImmutableMessage,
) SpecializedOwnedImmutableMessage[H, B] {
	msg := MustAsSpecializedImmutableMessage[H, B](owner.Message())
	return &specializedOwnedImmutableMessage[H, B]{message: msg, owner: owner}
}

func MustAsSpecializedRetainedImmutableMessage[H proto.Message, B proto.Message](
	retained RetainedImmutableMessage,
) SpecializedRetainedImmutableMessage[H, B] {
	msg := MustAsSpecializedImmutableMessage[H, B](retained.Message())
	return &specializedRetainedImmutableMessage[H, B]{message: msg, retained: retained}
}

// MustAsOwnedImmutableTxnMessage binds owner to its transaction message.
// The transaction and all of its child messages share one lifetime.
func MustAsOwnedImmutableTxnMessage(owner OwnedImmutableMessage) OwnedImmutableTxnMessage {
	txn := AsImmutableTxnMessage(owner.Message())
	if txn == nil {
		panic("failed to parse immutable transaction message")
	}
	return &ownedImmutableTxnMessage{message: txn, owner: owner}
}

func MustAsRetainedImmutableTxnMessage(retained RetainedImmutableMessage) RetainedImmutableTxnMessage {
	txn := AsImmutableTxnMessage(retained.Message())
	if txn == nil {
		panic("failed to parse immutable transaction message")
	}
	return &retainedImmutableTxnMessage{message: txn, retained: retained}
}

var (
	_ OwnedImmutableMessage       = (*ownedImmutableMessage)(nil)
	_ RetainedImmutableMessage    = (*retainedImmutableMessage)(nil)
	_ OwnedImmutableTxnMessage    = (*ownedImmutableTxnMessage)(nil)
	_ RetainedImmutableTxnMessage = (*retainedImmutableTxnMessage)(nil)
)
