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
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/netflix/rend/client/binprot"
	"github.com/netflix/rend/client/common"
	"github.com/netflix/rend/client/f"
	_ "github.com/netflix/rend/client/sigs"
	"github.com/netflix/rend/client/stats"
	"github.com/netflix/rend/client/textprot"
)

type metric struct {
	d  time.Duration
	op common.Op
}

var (
	taskPool = &sync.Pool{
		New: func() interface{} {
			return &common.Task{}
		},
	}

	metricPool = &sync.Pool{
		New: func() interface{} {
			return metric{}
		},
	}

	bkpool = &sync.Pool{
		New: func() interface{} {
			return make([][]byte, 0, 26)
		},
	}
)

func init() {
	go http.ListenAndServe("localhost:11337", nil)
}

func main() {
	if f.Pprof != "" {
		f, err := os.Create(f.Pprof)
		if err != nil {
			panic(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var prot common.Prot
	var numCmds int
	var usedCmds string
	var protString string

	if f.Binary {
		var b binprot.BinProt
		prot = b
		numCmds = 8
		usedCmds = "get, batch get, get and touch, set, add, replace, touch, delete"
		protString = "binary"
	} else {
		var t textprot.TextProt
		prot = t
		numCmds = 7
		usedCmds = "get, batch get, set, add, replace, touch, delete"
		protString = "text"
	}

	fmt.Printf("Performing %v operations total with:\n\t%v communication goroutines\n\tcommands %v\n\tover the %v protocol\n\n", f.NumOps, f.NumWorkers, usedCmds, protString)

	tasks := make(chan *common.Task)
	taskGens := new(sync.WaitGroup)
	comms := new(sync.WaitGroup)

	// TODO: Better math
	opsPerTask := f.NumOps / numCmds / f.NumWorkers

	// HUGE channel so the comm threads never block
	metrics := make(chan metric, f.NumOps)

	// spawn task generators
	for i := 0; i < f.NumWorkers; i++ {
		taskGens.Add(numCmds)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Set)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Add)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Replace)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Get)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Bget)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Delete)
		go cmdGenerator(tasks, taskGens, opsPerTask, common.Touch)

		if f.Binary {
			go cmdGenerator(tasks, taskGens, opsPerTask, common.Gat)
		}
	}

	// spawn communicators
	for i := 0; i < f.NumWorkers; i++ {
		comms.Add(1)
		conn, err := common.Connect(f.Host, f.Port)

		if err != nil {
			i--
			comms.Add(-1)
			continue
		}

		go communicator(prot, conn, tasks, metrics, comms)
	}

	summaries := &sync.WaitGroup{}
	summaries.Add(1)
	go func() {
		// consolidate some metrics
		// bucketize the timings based on operation
		// print min, max, average, 50%, 90%
		cons := make(map[common.Op][]int)
		for m := range metrics {
			if _, ok := cons[m.op]; ok {
				cons[m.op] = append(cons[m.op], int(m.d.Nanoseconds()))
			} else {
				cons[m.op] = []int{int(m.d.Nanoseconds())}
			}

			metricPool.Put(m)
		}

		for _, op := range common.AllOps {
			if f.Text && op == common.Gat {
				continue
			}

			times := cons[op]
			sort.Ints(times)
			s := stats.Get(times)

			fmt.Println()
			fmt.Println(op.String())
			fmt.Printf("Min: %fms\n", s.Min)
			fmt.Printf("Max: %fms\n", s.Max)
			fmt.Printf("Avg: %fms\n", s.Avg)
			fmt.Printf("p50: %fms\n", s.P50)
			fmt.Printf("p75: %fms\n", s.P75)
			fmt.Printf("p90: %fms\n", s.P90)
			fmt.Printf("p95: %fms\n", s.P95)
			fmt.Printf("p99: %fms\n", s.P99)
			fmt.Println()

			stats.PrintHist(times)
		}
		summaries.Done()
	}()

	// First wait for all the tasks to be generated,
	// then close the channel so the comm threads complete
	fmt.Println("Waiting for taskGens.")
	taskGens.Wait()

	fmt.Println("Task gens done.")
	close(tasks)

	fmt.Println("Tasks closed, waiting on comms.")
	comms.Wait()

	fmt.Println("Comms done.")
	close(metrics)

	summaries.Wait()
}

func cmdGenerator(tasks chan<- *common.Task, taskGens *sync.WaitGroup, numTasks int, cmd common.Op) {
	r := rand.New(rand.NewSource(common.RandSeed()))

	for i := 0; i < numTasks; i++ {
		task := taskPool.Get().(*common.Task)
		task.Cmd = cmd
		task.Key = common.RandData(r, f.KeyLength, false)
		task.Value = taskValue(r, cmd)
		tasks <- task
	}

	taskGens.Done()
}

func taskValue(r *rand.Rand, cmd common.Op) []byte {
	if cmd == common.Set || cmd == common.Add || cmd == common.Replace {
		// Random length between 1k and 10k
		valLen := r.Intn(9*1024) + 1024
		return common.RandData(r, valLen, true)
	}

	return nil
}

func communicator(prot common.Prot, conn net.Conn, tasks <-chan *common.Task, metrics chan<- metric, comms *sync.WaitGroup) {
	r := rand.New(rand.NewSource(common.RandSeed()))
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for item := range tasks {
		var err error
		start := time.Now()

		switch item.Cmd {
		case common.Set:
			err = prot.Set(rw, item.Key, item.Value)
		case common.Add:
			err = prot.Add(rw, item.Key, item.Value)
		case common.Replace:
			err = prot.Replace(rw, item.Key, item.Value)
		case common.Get:
			err = prot.Get(rw, item.Key)
		case common.Gat:
			err = prot.GAT(rw, item.Key)
		case common.Bget:
			bk := batchkeys(r, item.Key)
			err = prot.BatchGet(rw, bk)
			bkpool.Put(bk)
		case common.Delete:
			err = prot.Delete(rw, item.Key)
		case common.Touch:
			err = prot.Touch(rw, item.Key)
		}

		if err != nil {
			// don't print get misses, adds not stored, and replaces not stored
			if !(err == binprot.ErrKeyNotFound || err == binprot.ErrKeyExists) {
				fmt.Printf("Error performing operation %s on key %s: %s\n", item.Cmd, item.Key, err.Error())
			}
			// if the socket was closed, stop. Otherwise keep on hammering.
			if err == io.EOF {
				break
			}
		}

		m := metricPool.Get().(metric)
		m.d = time.Since(start)
		m.op = item.Cmd
		metrics <- m

		taskPool.Put(item)
	}

	conn.Close()
	comms.Done()
}

func batchkeys(r *rand.Rand, key []byte) [][]byte {
	key = key[1:]
	retval := bkpool.Get().([][]byte)
	retval = retval[:0]
	numKeys := byte(r.Intn(24) + 2 + int('A'))

	for i := byte('A'); i < numKeys; i++ {
		retval = append(retval, append([]byte{i}, key...))
	}

	return retval
}
