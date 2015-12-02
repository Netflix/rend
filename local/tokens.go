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
/**
 * Background goroutine to keep 1000 tokens around for heavy write loads
 */
package local

import "crypto/rand"

// Tokens are used during set handling to uniquely identify
// a specific set
var tokens chan *[16]byte

func genTokens() [16]byte {
	for {
		retval := new([16]byte)
		rand.Read(retval[:])
		tokens <- retval
	}
}

func init() {
	// keep 1000 unique tokens around for write-heavy loads
	// otherwise we have to wait on a read from /dev/urandom
	tokens = make(chan *[16]byte, 1000)
	go genTokens()
}
