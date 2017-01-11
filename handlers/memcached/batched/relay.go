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
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	connBatchSize            = 10
	connReadBufSize          = 8192
	connWriteBufSize         = 32768
	loadFactorHeuristicRatio = 0.75
)

var (
	relays    = make(map[string]*relay)
	relayLock = new(sync.RWMutex)
)

type relay struct {
	sock        string
	conns       atomic.Value
	addConnLock *sync.Mutex
	expand      chan struct{}
}

// Creates a new relay with one connection
func getRelay(sock string) *relay {
	relayLock.RLock()
	if r, ok := relays[sock]; ok {
		relayLock.RUnlock()
		return r
	}
	relayLock.RUnlock()

	// Lock here because we are creating a new relay for the given socket path
	// The rest of the new connections will block here and then pick it up on
	// the double check
	relayLock.Lock()

	// double check
	if r, ok := relays[sock]; ok {
		relayLock.Unlock()
		return r
	}

	// Create a new relay and wait for the first connection to be established
	// so it's usable.
	r := &relay{
		sock:        sock,
		conns:       atomic.Value{},
		addConnLock: new(sync.Mutex),
		expand:      make(chan struct{}, 1),
	}

	// initialize the atomic value
	r.conns.Store(make([]conn, 0))

	firstConnSetup := make(chan struct{})
	go r.monitor(firstConnSetup)
	<-firstConnSetup

	relays[sock] = r
	relayLock.Unlock()

	return r
}

// Adds a connection to the pool. This is one way only, making this effectively
// a high-water-mark pool with no connections being torn down.
func (r *relay) addConn() {
	// Ensure there's no races when adding a connection
	r.addConnLock.Lock()
	defer r.addConnLock.Unlock()

	c, err := net.Dial("unix", r.sock)
	if err != nil {
		// uh oh...
	}

	poolconn := newConn(c, 500, connBatchSize, connReadBufSize, connWriteBufSize)

	// Add the new connection (but with a new slice header)
	temp := r.conns.Load().([]conn)
	temp = append(temp, poolconn)

	// Store the modified slice
	r.conns.Store(temp)
}

// Submits a request to a random connection in the pool. The random number generator
// is passed in so there is no sharing between external connections.
func (r *relay) submit(rand *rand.Rand, req request) {
	// use rand to select a connection to submit to
	// the connection should notify the frontend by the channel
	// in the request struct
	cs := r.conns.Load().([]conn)
	idx := rand.Intn(len(cs))
	c := cs[idx]
	c.reqchan <- req
}

func (r *relay) monitor(firstConnSetup chan struct{}) {
	// do monitoring stuff
	// keep track of queue depths
	// this will need some handles to all the connections
	// atomic swap 0 in to each high water mark gauge
	// take max of all of them
	// since batch size is universal, we can compare against that
	// if the historical trend (5 times checked?) is above, say, 80% of capacity
	// add another connection
	// maybe double if maxxed out and add a single if above a lower limit

	// create first connection and notify after it's complete
	r.addConn()
	firstConnSetup <- struct{}{}

	for {
		// Re-evaluate either after 30 seconds or when a connection notifies that
		// it is overloaded.
		select {
		case <-time.After(30 * time.Second):
		case <-r.expand:
		}

		cs := r.conns.Load().([]conn)
		maxes := make([]uint32, len(cs))

		// Extract the maximum batch sizes seen since the last check
		for i, c := range cs {
			maxes[i] = atomic.SwapUint32(c.maxBatchSize, 0)
		}

		// Heuristic: calculate the percentage of the total batch capacity used
		var used uint64
		for _, u := range maxes {
			used += uint64(u)
		}

		total := float64(len(cs)) * connBatchSize
		loadFactor := float64(used) / total

		// if we are over our load factor ratio, add a connection
		if loadFactor > loadFactorHeuristicRatio {
			r.addConn()
		}
	}
}
