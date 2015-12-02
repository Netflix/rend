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
	if f.Binary {
		var b binprot.BinProt
		prot = b
	} else {
		var t textprot.TextProt
		prot = t
	}

	var wg sync.WaitGroup
	wg.Add(f.NumWorkers)

	for i := 0; i < f.NumWorkers; i++ {
		go func(prot common.Prot, wg sync.WaitGroup) {
			conn, err := common.Connect("localhost", f.Port)
			if err != nil {
				panic("Couldn't connect")
			}

			// 0 to 100k data
			for i := 0; i < 102400; i++ {
				key := common.RandData(f.KeyLength)
				value := common.RandData(i)

				prot.Set(conn, key, value)
				prot.Get(conn, key)
			}

			fmt.Println("Done.")
			wg.Done()
		}(prot, wg)
	}

	wg.Wait()
}
