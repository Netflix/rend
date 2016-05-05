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
	"flag"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/netflix/rend/client/binprot"
	"github.com/netflix/rend/client/common"
	_ "github.com/netflix/rend/client/sigs"
	"github.com/netflix/rend/client/textprot"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	var prot common.Prot

	// Connectivity / protocol options
	binary := flag.Bool("binary", false, "Use the binary protocol. Default is text.")
	udsock := flag.String("uds", "", "Unix domain socket to connect to. Takes precedence over --host and --port.")
	port := flag.Int("port", 11212, "Port to connect to.")
	host := flag.String("host", "localhost", "Hostname / IP to connect to.")

	// Operation details
	op := flag.String("op", "", "Operation to perform (required). One of: get, gete, bget, gat, set, add, replace, delete, touch, noop, version, quit\n"+
		"The gete protocol extension is only available when sending requests to a rend server.\n"+
		"For bget, 2-26 keys will be generated based on the given key by replacing the first letter with a-z")
	keyflag := flag.String("key", "", "The key to use. Used for get, gete, set, add, replace, delete, touch, gat")
	//flags := flag.Int("flags", 0, "Flags to send. Used for set, add, replace")
	//ttl := flag.Int("ttl", 0, "TTL to use. Used for set, add, replace, gat, touch")
	dataflag := flag.String("data", "", "Data to send. Used for set, add, replace")

	flag.Parse()

	key := []byte(*keyflag)
	data := []byte(*dataflag)

	if *binary {
		prot = binprot.BinProt{}
	} else {
		prot = textprot.TextProt{}
	}

	var conn net.Conn
	var err error
	if *udsock != "" {
		conn, err = net.Dial("unix", *udsock)
	} else {
		conn, err = net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to memcached.")

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	start := time.Now()

	var ret []byte
	switch *op {
	case "get":
		ret, err = prot.Get(rw, key)
	case "gete":
		ret, err = prot.GAT(rw, key)
	case "bget":
		bk := batchkeys(key)
		_, err = prot.BatchGet(rw, bk)
	case "gat":
		ret, err = prot.GAT(rw, key)
	case "set":
		err = prot.Set(rw, key, data)
	case "add":
		err = prot.Add(rw, key, data)
	case "replace":
		err = prot.Replace(rw, key, data)
	case "delete":
		err = prot.Delete(rw, key)
	case "touch":
		err = prot.Touch(rw, key)
	case "noop":
		//err = prot.Noop(rw)
	case "version":
		//err = prot.Version(rw)
	case "quit":
		//err = prot.Quit(rw)
	}

	duration := time.Since(start)
	fmt.Printf("Operation took %fms\n", float64(duration)/1000000.0)
	if ret != nil {
		fmt.Printf("Returned data: %s\n", string(ret))
	}

	if err != nil {
		fmt.Printf("Non-0 status performing operation %s on key %s: %s\n", *op, key, err.Error())
	} else {
		fmt.Println("Success")
	}

}

func batchkeys(key []byte) [][]byte {
	key = key[1:]
	var retval [][]byte
	numKeys := byte(rand.Intn(24) + 2 + int('A'))

	for i := byte('A'); i < numKeys; i++ {
		retval = append(retval, append([]byte{i}, key...))
	}

	return retval
}
