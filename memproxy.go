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

import "bufio"
import "fmt"
import "io"
import "net"
import "net/http"
import _ "net/http/pprof"
import "os"
import "os/signal"
import "runtime"
import "strings"

import "github.com/netflix/rend/binprot"
import "github.com/netflix/rend/common"
import "github.com/netflix/rend/handlers"
import "github.com/netflix/rend/handlers/memcached"
import "github.com/netflix/rend/textprot"

// Setting up signal handlers
func init() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		panic("Keyboard Interrupt")
	}()
}

// Set up profiling endpoint
func init() {
	go http.ListenAndServe("localhost:11299", nil)
}

func main() {
	server, err := net.Listen("tcp", ":11211")

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for {
		remote, err := server.Accept()

		if err != nil {
			fmt.Println(err.Error())
			remote.Close()
			continue
		}

		l1conn, err := net.Dial("unix", "/tmp/memcached.sock")

		if err != nil {
			fmt.Println(err.Error())
			if l1conn != nil {
				l1conn.Close()
			}
			remote.Close()
			continue
		}

		l1 := memcached.NewChunkedHandler(l1conn)
		//l1 := memcached.NewHandler(l1conn)

		go handleConnection(remote, l1, nil)
	}
}

func abort(toClose []io.Closer, err error, binary bool) {
	if err != io.EOF {
		fmt.Println("Error while processing request. Closing connection. Error:", err.Error())
	}
	// use proper serializer to respond here
	for _, c := range toClose {
		if c != nil {
			c.Close()
		}
	}
	//panic(err)
}

func identifyPanic() string {
	var name, file string
	var line int
	var pc [16]uintptr

	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
	}

	return fmt.Sprintf("Panic occured at: %v:%v (line %v)", file, name, line)
}

func handleConnection(remoteConn net.Conn, l1, l2 handlers.Handler) {
	defer func() {
		if r := recover(); r != nil {
			if r != io.EOF {
				fmt.Println("Recovered from runtime panic:", r)
				fmt.Println("Panic location: ", identifyPanic())
			}
		}
	}()

	handleConnectionReal(remoteConn, l1, l2)
}

func handleConnectionReal(remoteConn net.Conn, l1, l2 handlers.Handler) {
	remoteReader := bufio.NewReader(remoteConn)
	remoteWriter := bufio.NewWriter(remoteConn)

	var reqParser common.RequestParser
	var responder common.Responder
	var reqType common.RequestType
	var request interface{}

	binaryParser := binprot.NewBinaryParser(remoteReader)
	binaryResponder := binprot.NewBinaryResponder(remoteWriter)
	textParser := textprot.NewTextParser(remoteReader)
	textResponder := textprot.NewTextResponder(remoteWriter)

	for {
		binary, err := isBinaryRequest(remoteReader)

		if err != nil {
			abort([]io.Closer{remoteConn, l1, l2}, err, binary)
			return
		}

		if binary {
			reqParser = binaryParser
			responder = binaryResponder
		} else {
			reqParser = textParser
			responder = textResponder
		}

		request, reqType, err = reqParser.Parse()

		if err != nil {
			abort([]io.Closer{remoteConn, l1, l2}, err, binary)
			return
		}

		// TODO: handle nil
		switch reqType {
		case common.RequestSet:
			err = l1.Set(request.(common.SetRequest), remoteReader)

			if err == nil {
				responder.Set()
			}

		case common.RequestDelete:
			err = l1.Delete(request.(common.DeleteRequest))

			if err == nil {
				responder.Delete()
			}

		case common.RequestTouch:
			err = l1.Touch(request.(common.TouchRequest))

			if err == nil {
				responder.Touch()
			}

		case common.RequestGet:
			getReq := request.(common.GetRequest)
			resChan, errChan := l1.Get(getReq)

			for {
				select {
				case res, ok := <-resChan:
					if !ok {
						resChan = nil
					} else {
						if res.Miss {
							responder.GetMiss(res)
						} else {
							responder.Get(res)
						}
					}

				case getErr, ok := <-errChan:
					if !ok {
						errChan = nil
					} else {
						err = getErr
					}
				}

				if resChan == nil && errChan == nil {
					break
				}
			}

			responder.GetEnd(getReq.NoopEnd)

		case common.RequestGat:
			res, err := l1.GAT(request.(common.GATRequest))

			if err == nil {
				if res.Miss {
					responder.GATMiss(res)
				} else {
					responder.GAT(res)
					responder.GetEnd(false)
				}
			}

		case common.RequestUnknown:
			err = common.ErrUnknownCmd
		}

		// TODO: distinguish fatal errors from non-fatal
		if err != nil {
			if common.IsAppError(err) {
				responder.Error(err)
			} else {
				abort([]io.Closer{remoteConn, l1, l2}, err, binary)
				return
			}
		}
	}
}

func isBinaryRequest(reader *bufio.Reader) (bool, error) {
	headerByte, err := reader.Peek(1)
	if err != nil {
		return false, err
	}
	return int(headerByte[0]) == binprot.MagicRequest, nil
}
