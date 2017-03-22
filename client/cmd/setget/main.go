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
	"bytes"
	"io"
	"log"
	"math"
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
	"github.com/netflix/rend/timer"
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

	// create and fill key channels to drive workers
	perChan := int(math.Ceil(float64(f.NumOps) / float64(f.NumWorkers)))
	chans := make([]chan []byte, f.NumWorkers)
	for i := range chans {
		chans[i] = make(chan []byte, perChan)
	}
	fillKeys(chans)

	log.Println("Done generating keys")

	start := timer.Now()
	// spawn worker goroutines
	for i := 0; i < f.NumWorkers; i++ {
		conn, err := common.Connect(f.Host, f.Port)
		if err != nil {
			panic(err.Error())
		}
		rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		go worker(prot, rw, chans[i], wg)
	}

	wg.Wait()

	log.Println("Total comm time:", timer.Since(start))
}

// fills a bunch of channels round robin with keys
func fillKeys(chans []chan []byte) {
	totalCap := 0
	for _, c := range chans {
		totalCap += cap(c)
	}
	if totalCap < f.NumOps {
		panic("Channels cannot hold all the ops. Deadlock guaranteed.")
	}

	ci := 0
	key := bytes.Repeat([]byte{byte('A')}, f.KeyLength)
	for i := 0; i < f.NumOps; i++ {
		key = nextKey(key)
		chans[ci] <- key
		ci = (ci + 1) % len(chans)
	}

	// close them as they have all the data they need
	for _, c := range chans {
		close(c)
	}
}

func nextKey(key []byte) []byte {
	// make a copy to avoid touching keys in use
	temp := make([]byte, len(key))
	copy(temp, key)
	key = temp

	for i := 0; i < len(key); i++ {
		key[i] -= byte('A')
	}

	// ripple carry adder anyone?
	i := len(key) - 1
	carry := true
	for carry && i >= 0 {
		key[i] = (key[i] + 1) % 26
		carry = key[i] == 0
		i--
	}

	for i := 0; i < len(key); i++ {
		key[i] += byte('A')
	}

	return key
}

var opCount = new(uint64)

func worker(prot common.Prot, rw *bufio.ReadWriter, keys chan []byte, wg *sync.WaitGroup) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for key := range keys {
		curOpCount := atomic.AddUint64(opCount, 1)
		if curOpCount%10000 == 0 {
			log.Println(curOpCount)
		}

		valLen := r.Intn(9*1024) + 1024
		value := common.RandData(nil, valLen, true)

		// continue on even if there's errors here
		if err := prot.Set(rw, key, value); err != nil {
			log.Println("Error during set:", err.Error())
			if err == io.EOF {
				log.Println("End of file. Aborting!")
				wg.Done()
				return
			}
		}

		opaque := r.Int()

		// pass the test if the data matches
		ret, err := prot.GetWithOpaque(rw, key, opaque)
		if err != nil {
			log.Println("Error getting data for key", string(key), ":", err.Error())
			if err == io.EOF {
				log.Println("End of file. Aborting!")
				wg.Done()
				return
			}
			continue
		}

		if !bytes.Equal(value, ret) {
			log.Println("Data returned from server does not match!",
				"\nData len sent:", len(value),
				"\nData len recv:", len(ret))
			//log.Println("sent:", string(value))
			//log.Println("got:", string(ret))
		}
	}

	wg.Done()
}
