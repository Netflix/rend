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
	"github.com/netflix/rend/handlers/memcached/batched"
	"github.com/netflix/rend/handlers/memcached/chunked"
	"github.com/netflix/rend/handlers/memcached/std"
)

// Regular returns an implementation of the Handler interface that does standard,
// direct interactions with the external memcached backend which is listening on
// the specified unix domain socket.
func Regular(sock string) handlers.HandlerConst {
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

// Chunked returns an implementation of the Handler interface that implements an
// interaction model which splits data to set size chunks before inserting. the
// external memcached backend is expected to be listening on the specified unix
// domain socket.
func Chunked(sock string) handlers.HandlerConst {
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

// Batched returns an implementation of the Handler interface that multiplexes
// requests on to a connection pool in order to reduce the overhead per request.
func Batched(sock string) handlers.HandlerConst {
	return func() (handlers.Handler, error) {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			log.Println("Error opening connection:", err.Error())
			if conn != nil {
				conn.Close()
			}
			return nil, err
		}
		return batched.NewHandler(conn), nil
	}
}
