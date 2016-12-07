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

package batched

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

var (
	conns       = atomic.Value{}
	addConnLock = new(sync.Mutex)
)

// Adds a connection to the pool. This is one way only, making this effectively
// a high-water-mark pool with no connections being torn down.
func addConn(conn) {
	addConnLock.Lock()
	defer addConnLock.Unlock()

	temp := conns.Load().([]conn)
	temp = append(temp)

	conns.Store(temp)
}

// Submits a request to a random connection in the pool. The random number generator
// is passed in so there is no sharing between external connections.
func submit(req request, rand rand.Rand) {
	// use rand to select a connection to submit to
	// the connection should notify the frontend by the channel
	// in the request struct
	cs := conns.Load().([]conn)
	idx := rand.Intn(len(cs))
	c := cs[idx]
	c.reqchan <- req
}
