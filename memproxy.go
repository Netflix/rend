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

package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"

	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/inmem"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/server"
)

func init() {
	// Set GOGC default explicitly
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}

	// Setting up signal handlers
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		panic("Keyboard Interrupt")
	}()

	// http debug and metrics endpoint
	go http.ListenAndServe("localhost:11299", nil)

	// metrics output prefix
	metrics.SetPrefix("rend_")
}

// Flags
var (
	chunked bool
	l1sock  string
	l1inmem bool

	l2enabled bool
	l2sock    string

	locked      bool
	concurrency int
	multiReader bool

	port            int
	batchPort       int
	useDomainSocket bool
	sockPath        string
)

func init() {
	flag.BoolVar(&chunked, "chunked", false, "If --chunked is specified, the chunked handler is used for L1")
	flag.BoolVar(&l1inmem, "l1-inmem", false, "Use the debug in-memory in-process L1 cache")
	flag.StringVar(&l1sock, "l1-sock", "invalid.sock", "Specifies the unix socket to connect to L1")

	flag.BoolVar(&l2enabled, "l2-enabled", false, "Specifies if l2 is enabled")
	flag.StringVar(&l2sock, "l2-sock", "invalid.sock", "Specifies the unix socket to connect to L2. Only used if --l2-enabled is true.")

	flag.BoolVar(&locked, "locked", false, "Add locking to overall operations (above L1/L2 layers)")
	flag.IntVar(&concurrency, "concurrency", 8, "Concurrency level. 2^(concurrency) parallel operations permitted, assuming no collisions. Large values (>16) are likely useless and will eat up RAM. Default of 8 means 256 operations (on different keys) can happen in parallel.")
	flag.BoolVar(&multiReader, "multi-reader", true, "Allow (or disallow) multiple readers on the same key. If chunking is used, this will always be false and setting it to true will be ignored.")

	flag.IntVar(&port, "p", 11211, "External port to listen on")
	flag.IntVar(&batchPort, "bp", 11212, "External port to listen on for batch systems")
	flag.BoolVar(&useDomainSocket, "use-domain-socket", false, "Listen on a domain socket instead of a TCP port. --port will be ignored.")
	flag.StringVar(&sockPath, "sock-path", "/tmp/invalid.sock", "The socket path to listen on. Only valid in conjunction with --use-domain-socket.")

	flag.Parse()

	if concurrency >= 64 {
		panic("Concurrency cannot be more than 2^64")
	}
}

// And away we go
func main() {
	var l server.ListenArgs

	if useDomainSocket {
		l = server.ListenArgs{
			Type: server.ListenUnix,
			Path: sockPath,
		}
	} else {
		l = server.ListenArgs{
			Type: server.ListenTCP,
			Port: port,
		}
	}

	var o orcas.OrcaConst
	var h2 handlers.HandlerConst
	var h1 handlers.HandlerConst

	if l1inmem {
		h1 = inmem.New
	} else if chunked {
		h1 = memcached.Chunked(l1sock)
	} else {
		h1 = memcached.Regular(l1sock)
	}

	if l2enabled {
		o = orcas.L1L2
		h2 = memcached.Regular(l2sock)
	} else {
		o = orcas.L1Only
		h2 = handlers.NilHandler("")
	}

	// Add the locking wrapper if requested. The locking wrapper can either allow mutltiple readers
	// or not, with the same difference in semantics between a sync.Mutex and a sync.RWMutex. If
	// chunking is enabled, we want to ensure that stricter locking is enabled, since concurrent
	// sets into L1 with chunking can collide and cause data corruption.
	if locked {
		if chunked || !multiReader {
			o = orcas.Locked(o, false, uint8(concurrency))
		} else {
			o = orcas.Locked(o, true, uint8(concurrency))
		}
	}

	go server.ListenAndServe(l, server.Default, o, h1, h2)

	if l2enabled {
		// If L2 is enabled, start the batch L1 / L2 orchestrator
		l = server.ListenArgs{
			Type: server.ListenTCP,
			Port: batchPort,
		}

		go server.ListenAndServe(l, server.Default, orcas.L1L2Batch, h1, h2)
	}

	// Block forever
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
