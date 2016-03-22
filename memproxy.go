// Copyright 2015 Netflix, Inc.
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

	"github.com/netflix/rend/handlers"
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
var chunked bool
var l1sock string
var l2enabled bool
var l2sock string
var port int

func init() {
	flag.BoolVar(&chunked, "chunked", false, "If --chunked is specified, the chunked handler is used for L1")
	flag.StringVar(&l1sock, "l1-sock", "invalid.sock", "Specifies the unix socket to connect to L1")
	flag.BoolVar(&l2enabled, "l2-enabled", false, "Specifies if l2 is enabled")
	flag.StringVar(&l2sock, "l2-sock", "invalid.sock", "Specifies the unix socket to connect to L2. Only used if --l2-enabled is true.")
	flag.IntVar(&port, "p", 11211, "External port to listen on")
	flag.Parse()
}

// And away we go
func main() {
	l := server.ListenArgs{
		Port:      port,
		L1sock:    l1sock,
		L2enabled: l2enabled,
		L2sock:    l2sock,
	}

	var h1 handlers.HandlerConst
	if chunked {
		h1 = memcached.Chunked
	} else {
		h1 = memcached.Regular
	}

	server.ListenAndServe(l, server.Default, orcas.L1Only, h1, handlers.NilHandler)
}
