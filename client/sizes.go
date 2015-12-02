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

import "bufio"
import "fmt"
import "math/rand"
import "sync"
import "time"

import "./client/common"
import "./client/textprot"

func main() {
    var t textprot.TextProt
    var wg sync.WaitGroup

    rand.Seed(time.Now().UTC().UnixNano())

    wg.Add(10)
    for i := 0; i < 10; i++ {
        go doStuff(t, wg)
    }

    wg.Wait()
}

func doStuff(prot common.Prot, wg sync.WaitGroup) {
    conn, err := common.Connect("localhost", 11212)
    if err != nil {
        panic("Couldn't connect")
    }

    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)

    for i := 0; i < 10240; i++ {
        key := common.RandData(4)
        value := common.RandData(i)

        prot.Set(reader, writer, key, value)
        prot.Get(reader, writer, key)
    }

    fmt.Println("Done.")
    wg.Done()
}
