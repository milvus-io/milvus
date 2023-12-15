package typeutil

import "sync"

// GroupMutexGuard is a guard for GroupMutex
type GroupMutexGuard struct {
	mutex *sync.Mutex
}

// Unlock unlocks the mutex
func (g *GroupMutexGuard) Unlock() {
	g.mutex.Unlock()
}

// NewGroupMutex creates a new GroupMutex
func NewGroupMutex[K comparable]() *GroupMutex[K] {
	return &GroupMutex[K]{
		mutexes: NewConcurrentMap[K, *sync.Mutex](),
	}
}

// GroupMutex is a mutex that can be locked by a key
// Once a lock added, it will never clean.
type GroupMutex[K comparable] struct {
	mutexes *ConcurrentMap[K, *sync.Mutex]
}

// Lock locks the mutex by the key
func (m *GroupMutex[K]) Lock(key K) *GroupMutexGuard {
	mtx, _ := m.mutexes.GetOrInsert(key, &sync.Mutex{})
	mtx.Lock()

	return &GroupMutexGuard{
		mutex: mtx,
	}
}

// LockIfExist tries to lock the mutex if the key exists
func (m *GroupMutex[K]) LockIfExist(key K) *GroupMutexGuard {
	mtx, loaded := m.mutexes.Get(key)
	if !loaded {
		return nil
	}
	mtx.Lock()

	return &GroupMutexGuard{
		mutex: mtx,
	}
}

// Exists checks if the key exists
func (m *GroupMutex[K]) Exists(key K) bool {
	_, loaded := m.mutexes.Get(key)
	return loaded
}

// removableLocker is a locker that can be removed
type removableLocker struct {
	mutex sync.Mutex
	valid bool
}

// RemovableGroupMutexGuard is a guard for RemovableGroupMutex.
type RemovableGroupMutexGuard[K comparable] struct {
	key     K
	locker  *removableLocker
	mutexes *ConcurrentMap[K, *removableLocker]
}

// UnlockAndRemove unlocks the mutex and remove the locker from group mutex.
func (g *RemovableGroupMutexGuard[K]) UnlockAndRemove() {
	g.locker.valid = false
	g.locker.mutex.Unlock()
	g.mutexes.Remove(g.key)
}

// Unlock unlocks the mutex.
func (g *RemovableGroupMutexGuard[K]) Unlock() {
	g.locker.mutex.Unlock()
}

// NewRemovableGroupMutex creates a new RemovableGroupMutex
func NewRemovableGroupMutex[K comparable]() *RemovableGroupMutex[K] {
	return &RemovableGroupMutex[K]{
		mutexes: NewConcurrentMap[K, *removableLocker](),
	}
}

// Remove removes the locker
type RemovableGroupMutex[K comparable] struct {
	mutexes *ConcurrentMap[K, *removableLocker]
}

// Lock locks the mutex by the key
func (m *RemovableGroupMutex[K]) Lock(key K) *RemovableGroupMutexGuard[K] {
	for {
		// Do a busy loop to acquire the lock.
		// If the locker is invalid, it will be removed from the map as soon as possible.
		mtx, loaded := m.mutexes.GetOrInsert(key, &removableLocker{
			mutex: sync.Mutex{},
			valid: true,
		})
		_ = loaded
		mtx.mutex.Lock()

		// Check if the locker is valid
		if mtx.valid {
			return &RemovableGroupMutexGuard[K]{
				locker:  mtx,
				key:     key,
				mutexes: m.mutexes,
			}
		}
		// if mutex is invalid, unlock it and try to acquire again.
		mtx.mutex.Unlock()
	}
}

// LockIfExist tries to lock the mutex if the key exists
func (m *RemovableGroupMutex[K]) LockIfExist(key K) *RemovableGroupMutexGuard[K] {
	mtx, loaded := m.mutexes.Get(key)
	if !loaded {
		return nil
	}

	mtx.mutex.Lock()
	if mtx.valid {
		return &RemovableGroupMutexGuard[K]{
			locker:  mtx,
			key:     key,
			mutexes: m.mutexes,
		}
	}
	mtx.mutex.Unlock()
	return nil
}

// Exists checks if the key exists
func (m *RemovableGroupMutex[K]) Exists(key K) bool {
	mtx, loaded := m.mutexes.Get(key)
	if !loaded {
		return false
	}
	mtx.mutex.Lock()
	exist := mtx.valid
	mtx.mutex.Unlock()
	return exist
}
