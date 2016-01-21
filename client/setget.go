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

import "fmt"
import "io"
import "math/rand"
import "time"
import "sync"

import "./common"
import "./f"
import _ "./sigs"
import "./binprot"
import "./textprot"

// Package init
func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
	var prot common.Prot
	if f.Binary {
		var b binprot.BinProt
		prot = b
	} else {
		var t textprot.TextProt
		prot = t
	}

	tasks := make(chan *common.Task)
	wg := new(sync.WaitGroup)
	wg.Add(f.NumWorkers)

	// spawn worker goroutines
	for i := 0; i < f.NumWorkers; i++ {
		conn, err := common.Connect("localhost", f.Port)
		if err != nil {
			fmt.Println("Error:", err.Error())
		}
		go worker(prot, conn, tasks, wg)
	}

	source := common.RandData(10240)

	for i := 0; i < f.NumOps; i++ {
		// Random length between 1k and 10k
		valLen := rand.Intn(9*1024) + 1024

		tasks <- &common.Task{
			Key:   common.RandData(f.KeyLength),
			Value: source[:valLen],
		}

		if i%10000 == 0 {
			fmt.Printf("%v\r\n", i)
		}
	}

	close(tasks)
	wg.Wait()
}

func worker(prot common.Prot, rw io.ReadWriter, tasks chan *common.Task, wg *sync.WaitGroup) {
	for item := range tasks {
		prot.Set(rw, item.Key, item.Value)
		prot.Get(rw, item.Key)
	}

	wg.Done()
}
