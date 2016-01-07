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

// Memproxy is a proxy for memcached that will split the data input
// into fixed-size chunks for storage. It will reassemble the data
// on retrieval with set.
package main

import "bufio"
import "fmt"
import "io"
import "net"
import "os"
import "os/signal"
import "runtime"
import "strings"

import "github.com/netflix/rend/binprot"
import "github.com/netflix/rend/common"
import "github.com/netflix/rend/handlers/memcached"
import "github.com/netflix/rend/textprot"

const verbose = false

func main() {

	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		panic("Keyboard Interrupt")
	}()

	server, err := net.Listen("tcp", ":11212")

	if err != nil {
		print(err.Error())
	}

	for {
		remote, err := server.Accept()

		if err != nil {
			fmt.Println(err.Error())
			remote.Close()
			continue
		}

		local, err := net.Dial("unix", "/tmp/memcached.sock")

		if err != nil {
			fmt.Println(err.Error())
			if local != nil {
				local.Close()
			}
			remote.Close()
			continue
		}

		go handleConnection(remote, local, nil)
	}
}

func abort(remote, local net.Conn, err error, binary bool) {
	if err != io.EOF {
		fmt.Println("Error while processing request. Closing connection. Error:", err.Error())
	}
	// use proper serializer to respond here
	remote.Close()
	local.Close()
	panic(err)
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

	return fmt.Sprintf("%v:%v:%v", file, name, line)
}

func handleConnection(remoteConn, l1, l2 net.Conn) {
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

func handleConnectionReal(remoteConn, l1, l2 net.Conn) {
	remoteReader := bufio.NewReader(remoteConn)
	remoteWriter := bufio.NewWriter(remoteConn)
	l1RW := bufio.NewReadWriter(bufio.NewReader(l1), bufio.NewWriter(l1))

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
			abort(remoteConn, l1, err, binary)
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
			abort(remoteConn, l1, err, binary)
			return
		}

		// TODO: handle nil
		switch reqType {
		case common.REQUEST_SET:
			err = local.HandleSet(request.(common.SetRequest), remoteReader, l1RW)

			if err == nil {
				// For text protocol, read in \r\n at end of data.
				// A little hacky, but oh well. Might be wrapped up in a
				// "cleaupSet" function or something
				if !binary {
					_, err = remoteReader.ReadString('\n')
				}

				if err == nil {
					responder.Set()
				}
			}

		case common.REQUEST_DELETE:
			err = local.HandleDelete(request.(common.DeleteRequest), l1RW)

			if err == nil {
				responder.Delete()
			}

		case common.REQUEST_TOUCH:
			err = local.HandleTouch(request.(common.TouchRequest), l1RW)

			if err == nil {
				responder.Touch()
			}

		case common.REQUEST_GET:
			getReq := request.(common.GetRequest)
			resChan, errChan := local.HandleGet(getReq, l1RW)

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

		case common.REQUEST_GAT:
			res, err := local.HandleGAT(request.(common.GATRequest), l1RW)

			if err == nil {
				if res.Miss {
					responder.GATMiss(res)
				} else {
					responder.GAT(res)
					responder.GetEnd(false)
				}
			}

		case common.REQUEST_UNKNOWN:
			err = common.ERROR_UNKNOWN_CMD
		}

		// TODO: distinguish fatal errors from non-fatal
		if err != nil {
			if common.IsAppError(err) {
				responder.Error(err)
			} else {
				abort(remoteConn, l1, err, binary)
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
	return int(headerByte[0]) == binprot.MAGIC_REQUEST, nil
}
