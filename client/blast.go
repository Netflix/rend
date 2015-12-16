/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import "fmt"
import "io"
import "math/rand"
import "sync"
import "time"

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
	var numCmds int
	var usedCmds string
	var protString string
	
	if f.Binary {
		var b binprot.BinProt
		prot = b
		numCmds = 6
		usedCmds = "get, batch get, get and touch, set, touch, delete"
		protString = "binary"
	} else {
		var t textprot.TextProt
		prot = t
		numCmds = 5
		usedCmds = "get, batch get, set, touch, delete"
		protString = "text"
	}

	fmt.Printf("Performing %v operations total with:\n\t%v communication goroutines\n\tcommands %v\n\tover the %v protocol\n\n", f.NumOps, f.NumWorkers, usedCmds, protString)

	tasks := make(chan *common.Task)
	taskGens := new(sync.WaitGroup)
	comms := new(sync.WaitGroup)

	// TODO: Better math
	opsPerTask := f.NumOps / numCmds / f.NumWorkers

	// spawn task generators
	for i := 0; i < f.NumWorkers; i++ {
		taskGens.Add(numCmds)
		go cmdGenerator(tasks, taskGens, opsPerTask, "set")
		go cmdGenerator(tasks, taskGens, opsPerTask, "get")
		go cmdGenerator(tasks, taskGens, opsPerTask, "bget")
		go cmdGenerator(tasks, taskGens, opsPerTask, "delete")
		go cmdGenerator(tasks, taskGens, opsPerTask, "touch")

		if f.Binary {
			go cmdGenerator(tasks, taskGens, opsPerTask, "gat")
		}
	}

	// spawn communicators
	for i := 0; i < f.NumWorkers; i++ {
		comms.Add(1)
		conn, err := common.Connect("localhost", f.Port)

		if err != nil {
			i--
			comms.Add(-1)
			continue
		}

		go communicator(prot, conn, tasks, comms)
	}

	// First wait for all the tasks to be generated,
	// then close the channel so the comm threads complete
	fmt.Println("Waiting for taskGens.")
	taskGens.Wait()

	fmt.Println("Task gens done.")
	close(tasks)

	fmt.Println("Tasks closed, waiting on comms.")
	comms.Wait()

	fmt.Println("Comms done.")
}

func cmdGenerator(tasks chan<- *common.Task, taskGens *sync.WaitGroup, numTasks int, cmd string) {
	for i := 0; i < numTasks; i++ {
		tasks <- &common.Task{
			Cmd:   cmd,
			Key:   common.RandData(f.KeyLength),
			Value: taskValue(cmd),
		}
	}

	fmt.Println(cmd, "gen done")
	taskGens.Done()
}

func taskValue(cmd string) []byte {
	if cmd == "set" {
		// Random length between 1k and 10k
		valLen := rand.Intn(9*1024) + 1024
		return common.RandData(valLen)
	}

	return nil
}

func communicator(prot common.Prot, rw io.ReadWriter, tasks <-chan *common.Task, comms *sync.WaitGroup) {
	for item := range tasks {
		var err error

		switch item.Cmd {
		case "set":
			err = prot.Set(rw, item.Key, item.Value)
		case "get":
			err = prot.Get(rw, item.Key)
		case "gat":
			err = prot.GAT(rw, item.Key)
		case "bget":
			err = prot.BatchGet(rw, batchkeys(item.Key))
		case "delete":
			err = prot.Delete(rw, item.Key)
		case "touch":
			err = prot.Touch(rw, item.Key)
		}

		if err != nil {
			if err != binprot.ERR_KEY_NOT_FOUND {
				fmt.Printf("Error performing operation %s on key %s: %s\n", item.Cmd, item.Key, err.Error())
			}
			// if the socket was closed, stop. Otherwise keep on hammering.
			if err == io.EOF {
				break
			}
		}
	}

	fmt.Println("comm done")

	comms.Done()
}

func batchkeys(key []byte) [][]byte {
	key = key[1:]
	retval := make([][]byte, 0)
	numKeys := rand.Intn(25) + 2 + int('A')

	for i := int('A'); i < numKeys; i++ {
		retval = append(retval, append([]byte{byte(i)}, key...))
	}

	return retval
}
