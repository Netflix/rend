// Copyright 2016 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orcas

import (
	"hash"
	"hash/fnv"
	"sync"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

type LockingOrca struct {
	wrapped Orca
	locks   []sync.RWMutex
	//counts  []uint32
	hpool *sync.Pool
}

// Locking wraps an orcas.Orca to provide locking around operations on the same
// key. The concurrency param allows 2^(concurrency) operations to happen in
// parallel. E.g. concurrency of 1 would allow 2 parallel operations, while a
// concurrency of 4 allows 2^4 = 16 parallel operations.
func Locking(oc OrcaConst, concurrency uint8) OrcaConst {
	if concurrency < 0 {
		panic("Concurrency level must be at least 0")
	}

	// keep the same locks for all instances by closing over this slice
	locks := make([]sync.RWMutex, 1<<concurrency)
	//counts := make([]uint32, 1<<concurrency)
	pool := &sync.Pool{
		New: func() interface{} {
			return fnv.New32a()
		},
	}

	return func(l1, l2 handlers.Handler, res common.Responder) Orca {
		return &LockingOrca{
			wrapped: oc(l1, l2, res),
			locks:   locks,
			//counts:  counts,
			hpool: pool,
		}
	}
}

//var numops uint64 = 0

func (l *LockingOrca) getlock(key []byte) *sync.RWMutex {
	h := l.hpool.Get().(hash.Hash32)
	h.Reset()

	// Calculate bucket using hash and mod. FNV1a never returns an error.
	h.Write(key)
	bucket := int(h.Sum32())
	bucket &= len(l.locks) - 1

	//atomic.AddUint32(&l.counts[bucket], 1)

	//if (atomic.AddUint64(&numops, 1) % 10000) == 0 {
	//	for idx, count := range l.counts {
	//		fmt.Printf("%d: %d\n", idx, count)
	//	}
	//}

	return &l.locks[bucket]
}

func (l *LockingOrca) Set(req common.SetRequest) error {
	lock := l.getlock(req.Key)
	lock.Lock()
	ret := l.wrapped.Set(req)
	lock.Unlock()
	return ret
}

func (l *LockingOrca) Add(req common.SetRequest) error {
	lock := l.getlock(req.Key)
	lock.Lock()
	ret := l.wrapped.Add(req)
	lock.Unlock()
	return ret
}

func (l *LockingOrca) Replace(req common.SetRequest) error {
	lock := l.getlock(req.Key)
	lock.Lock()
	ret := l.wrapped.Replace(req)
	lock.Unlock()
	return ret
}

func (l *LockingOrca) Delete(req common.DeleteRequest) error {
	lock := l.getlock(req.Key)
	lock.Lock()
	ret := l.wrapped.Delete(req)
	lock.Unlock()
	return ret
}

func (l *LockingOrca) Touch(req common.TouchRequest) error {
	lock := l.getlock(req.Key)
	lock.Lock()
	ret := l.wrapped.Touch(req)
	lock.Unlock()
	return ret
}

func (l *LockingOrca) Get(req common.GetRequest) error {
	// Acquire read locks for all keys
	for _, key := range req.Keys {
		l.getlock(key).RLock()
	}

	ret := l.wrapped.Get(req)

	// Release all read locks
	for _, key := range req.Keys {
		l.getlock(key).RUnlock()
	}

	return ret
}

func (l *LockingOrca) GetE(req common.GetRequest) error {
	// Acquire read locks for all keys
	for _, key := range req.Keys {
		l.getlock(key).RLock()
	}

	ret := l.wrapped.GetE(req)

	// Release all read locks
	for _, key := range req.Keys {
		l.getlock(key).RUnlock()
	}

	return ret
}

func (l *LockingOrca) Gat(req common.GATRequest) error {
	lock := l.getlock(req.Key)
	lock.Lock()
	ret := l.wrapped.Gat(req)
	lock.Unlock()
	return ret
}

func (l *LockingOrca) Noop(req common.NoopRequest) error {
	return l.wrapped.Noop(req)
}

func (l *LockingOrca) Quit(req common.QuitRequest) error {
	return l.wrapped.Quit(req)
}

func (l *LockingOrca) Version(req common.VersionRequest) error {
	return l.wrapped.Version(req)
}

func (l *LockingOrca) Unknown(req common.Request) error {
	return l.wrapped.Unknown(req)
}

func (l *LockingOrca) Error(req common.Request, reqType common.RequestType, err error) {
	l.wrapped.Error(req, reqType, err)
}
