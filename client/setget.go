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
	"bufio"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/netflix/rend/client/binprot"
	"github.com/netflix/rend/client/common"
	"github.com/netflix/rend/client/f"
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

	tasks := make(chan *common.Task)
	wg := new(sync.WaitGroup)
	wg.Add(f.NumWorkers)

	// spawn worker goroutines
	for i := 0; i < f.NumWorkers; i++ {
		conn, err := common.Connect(f.Host, f.Port)
		if err != nil {
			panic(err.Error())
		}
		rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		go worker(prot, rw, tasks, wg)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < f.NumOps; i++ {
		// Random length between 1k and 10k
		valLen := r.Intn(9*1024) + 1024

		tasks <- &common.Task{
			Key:   common.RandData(r, f.KeyLength, false),
			Value: common.RandData(nil, valLen, true),
		}

		if i%10000 == 0 {
			log.Printf("%v\r\n", i)
		}
	}

	close(tasks)
	wg.Wait()
}

func worker(prot common.Prot, rw *bufio.ReadWriter, tasks chan *common.Task, wg *sync.WaitGroup) {
	for item := range tasks {
		prot.Set(rw, item.Key, item.Value)
		prot.Get(rw, item.Key)
	}

	wg.Done()
}
