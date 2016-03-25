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

package memcached

import (
	"log"
	"net"

	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/memcached/chunked"
	"github.com/netflix/rend/handlers/memcached/std"
)

func Regular(sock string) func() (handlers.Handler, error) {
	return func() (handlers.Handler, error) {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			return nil, err
		}
		return std.NewHandler(conn), nil
	}
}

func Chunked(sock string) func() (handlers.Handler, error) {
	return func() (handlers.Handler, error) {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			log.Println("Error opening connection:", err.Error())
			if conn != nil {
				conn.Close()
			}
			return nil, err
		}
		return chunked.NewHandler(conn), nil
	}
}
