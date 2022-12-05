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

package cache

// Cache implement based on https://github.com/goburrow/cache, which
// provides partial implementations of Guava Cache, mainly support LRU.

// Cache is a key-value cache which entries are added and stayed in the
// cache until either are evicted or manually invalidated.
// TODO: support async clean up expired data
type Cache[K comparable, V any] interface {
	// GetIfPresent returns value associated with Key or (nil, false)
	// if there is no cached value for Key.
	GetIfPresent(K) (V, bool)

	// Put associates value with Key. If a value is already associated
	// with Key, the old one will be replaced with Value.
	Put(K, V)

	// Invalidate discards cached value of the given Key.
	Invalidate(K)

	// InvalidateAll discards all entries.
	InvalidateAll()

	// Scan walk cache and apply a filter func to each element
	Scan(func(K, V) bool) map[K]V

	// Stats returns cache statistics.
	Stats() *Stats

	// Close implements io.Closer for cleaning up all resources.
	// Users must ensure the cache is not being used before closing or
	// after closed.
	Close() error
}

// Func is a generic callback for entry events in the cache.
type Func[K comparable, V any] func(K, V)

// LoadingCache is a cache with values are loaded automatically and stored
// in the cache until either evicted or manually invalidated.
type LoadingCache[K comparable, V any] interface {
	Cache[K, V]

	// Get returns value associated with Key or call underlying LoaderFunc
	// to load value if it is not present.
	Get(K) (V, error)

	// Refresh loads new value for Key. If the Key already existed, it will
	// sync refresh it. or this function will block until the value is loaded.
	Refresh(K) error
}

// LoaderFunc retrieves the value corresponding to given Key.
type LoaderFunc[K comparable, V any] func(K) (V, error)

type GetPreLoadDataFunc[K comparable, V any] func() (map[K]V, error)
