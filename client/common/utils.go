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
package common

import "fmt"
import "math/rand"
import "net"

// constants and configuration
// No constant arrays :(
var letters = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandData(n int) []byte {
    b := make([]byte, n)

    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }

    return b
}

func Connect(host string, port int) (net.Conn, error) {
    conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
    if err != nil { return nil, err }

    fmt.Println("Connected to memcached.")

    return conn, nil
}