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
	"bufio"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/netflix/rend/client/binprot"
	"github.com/netflix/rend/client/common"
	"github.com/netflix/rend/client/f"
	_ "github.com/netflix/rend/client/sigs"
	"github.com/netflix/rend/client/textprot"
)

func init() {
	go http.ListenAndServe("localhost:11337", nil)
}

func main() {
	var prot common.Prot
	if f.Binary {
		prot = binprot.BinProt{}
	} else {
		prot = textprot.TextProt{}
	}

	wg := new(sync.WaitGroup)
	wg.Add(f.NumWorkers)

	extraOps := f.NumOps % f.NumWorkers
	opsPerWorker := f.NumOps / f.NumWorkers

	start := time.Now()

	// spawn worker goroutines
	for i := 0; i < f.NumWorkers; i++ {
		conn, err := common.Connect(f.Host, f.Port)
		if err != nil {
			panic(err.Error())
		}

		rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		numops := opsPerWorker
		if i == 0 {
			numops += extraOps
		}

		go worker(numops, prot, rw, wg)
	}

	wg.Wait()

	log.Println("Total comm time:", time.Since(start))
}

var opCount = new(uint64)

func worker(numops int, prot common.Prot, rw *bufio.ReadWriter, wg *sync.WaitGroup) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numops; i++ {
		curOpCount := atomic.AddUint64(opCount, 1)
		if curOpCount%10000 == 0 {
			log.Println(curOpCount)
		}

		// Generate key anew each time
		key := common.RandData(r, f.KeyLength, false)

		// Value size between 5k and 20k
		valLen := r.Intn(15*1024) + 5*1024
		value := common.RandData(nil, valLen, true)

		// continue on even if there's errors here
		if err := prot.Set(rw, key, value); err != nil {
			log.Println("Error during set:", err.Error())
		}
	}

	wg.Done()
}
