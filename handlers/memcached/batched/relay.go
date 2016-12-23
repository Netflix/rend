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
	conns       = atomic.Value{}
	addConnLock = new(sync.Mutex)
	expand      = make(chan struct{}, 1)
)

type relay struct{}

// Creates a new relay with one connection
func newRelay() {
	firstConnSetup := make(chan struct{})
	go monitor(firstConnSetup)
	<-firstConnSetup
}

// Adds a connection to the pool. This is one way only, making this effectively
// a high-water-mark pool with no connections being torn down.
func addConn(c net.Conn) {
	// Ensure there's races when adding a connection
	addConnLock.Lock()
	defer addConnLock.Unlock()

	newConn(c, 500, connBatchSize, connReadBufSize, connWriteBufSize)

	// Add the new connection (but with a new slice header)
	temp := conns.Load().([]conn)
	temp = append(temp, conn)

	// Store the modified slice
	conns.Store(temp)
}

// Submits a request to a random connection in the pool. The random number generator
// is passed in so there is no sharing between external connections.
func (r relay) submit(req request, rand rand.Rand) {
	// use rand to select a connection to submit to
	// the connection should notify the frontend by the channel
	// in the request struct
	cs := conns.Load().([]conn)
	idx := rand.Intn(len(cs))
	c := cs[idx]
	c.reqchan <- req
}

func monitor(firstConnSetup chan struct{}) {
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
	addConn()
	firstConnSetup <- struct{}{}

	for {
		// Re-evaluate either after 30 seconds or when a connection notifies that
		// it is overloaded.
		select {
		case <-time.After(30 * time.Second):
		case <-expand:
		}

		cs := conns.Load().([]conn)
		maxes := make([]uint32, len(cs))

		// Extract the maximum batch sizes seen since the last check
		for i, c := range cs {
			maxes[i] = atomic.SwapUint32(c.maxBatchSize, 0)
		}

		// Heuristic: calculate the percentage of the total batch capacity used
		var used uint64
		for _, u := range maxes {
			used += u
		}

		total := float64(len(cs)) * connBatchSize
		loadFactor := flaot64(used) / total

		// if we are over our load factor ratio, add a connection
		if loadFactor > loadFactorHeuristicRatio {

		}
	}
}
