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
	"math/rand"
	"sync"

	"github.com/netflix/rend/client/binprot"
	"github.com/netflix/rend/client/common"
	"github.com/netflix/rend/client/f"
	"github.com/netflix/rend/client/textprot"
)

func main() {
	var prot common.Prot
	if f.Binary {
		var b binprot.BinProt
		prot = b
	} else {
		var t textprot.TextProt
		prot = t
	}

	wg := &sync.WaitGroup{}
	wg.Add(f.NumWorkers)

	for i := 0; i < f.NumWorkers; i++ {
		go func(prot common.Prot, wg *sync.WaitGroup, id int) {

			conn, err := common.Connect("localhost", f.Port)
			if err != nil {
				panic("Couldn't connect")
			}

			r := rand.New(rand.NewSource(common.RandSeed()))
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
			var i int

			defer func() {
				if r := recover(); r != nil {
					fmt.Println("UH OH", id, i)
				}
			}()

			// 0 to 1MB data in 100 byte increments
			for i = 0; i < 1048576; i += 100 {

				/*if i%10000 == 0 {
					fmt.Println(id, i)
				}*/

				key := common.RandData(r, f.KeyLength, false)
				value := common.RandData(nil, i, true)

				prot.Set(rw, key, value)
				prot.Get(rw, key)
			}

			fmt.Println("Done.")
			wg.Done()
		}(prot, wg, i)
	}

	wg.Wait()
}
