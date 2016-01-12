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

package common

import "bytes"
import crand "crypto/rand"
import "encoding/binary"
import "fmt"
import "math/rand"
import "net"
import "time"

// constants and configuration
// No constant arrays :(
var letters = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

const PREDATA_LENGTH = 20 * 1024

var predata []byte

func init() {
	r := rand.New(rand.NewSource(RandSeed()))
	predata = RandData(r, PREDATA_LENGTH, false)
}

func RandSeed() int64 {
	b := make([]byte, 8)
	if _, err := crand.Read(b); err != nil {
		panic(err.Error())
	}
	buf := bytes.NewBuffer(b)
	var ret int64
	binary.Read(buf, binary.LittleEndian, &ret)
	return ret
}

func RandData(r *rand.Rand, n int, useCached bool) []byte {
	if useCached && n <= PREDATA_LENGTH {
		return predata[:n]
	}

	b := make([]byte, n)

	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}

	return b
}

func Connect(host string, port int) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to memcached.")

	return conn, nil
}

const MAX_TTL = 3600

func init() {
	rand.Seed(time.Now().UnixNano())
}

// get a random expiration
func Exp() uint32 {
	return uint32(rand.Intn(MAX_TTL))
}
