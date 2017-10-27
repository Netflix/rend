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

package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
)

type tcpListener struct {
	listener net.Listener
}

func (l *tcpListener) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *tcpListener) ModifyConnSettings(conn net.Conn) error {
	tcpRemote := conn.(*net.TCPConn)

	if err := tcpRemote.SetKeepAlive(true); err != nil {
		return err
	}

	return tcpRemote.SetKeepAlivePeriod(30 * time.Second)
}

// TCPListener is a ListenConst that returns a tcp listener for the given port
func TCPListener(port int) ListenConst {
	return func() (Listener, error) {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, fmt.Errorf("Error binding to port %d: %v", port, err.Error())
		}
		return &tcpListener{listener: listener}, nil
	}
}

type unixListener struct {
	listener net.Listener
}

func (l *unixListener) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *unixListener) ModifyConnSettings(conn net.Conn) error {
	return nil
}

// UnixListener is a ListenConst that returns a unix domain socket listener for the given path
func UnixListener(path string) ListenConst {
	return func() (Listener, error) {
		err := os.Remove(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("Error removing previous unix socket file at %s", path)
		}

		listener, err := net.Listen("unix", path)
		if err != nil {
			return nil, fmt.Errorf("Error binding to unix socket at %s: %v", path, err.Error())
		}

		return &unixListener{listener: listener}, nil
	}
}

// ListenAndServe is the main accept() loop of a server. It will use all of the components passed in
// to construct a full set of components to serve a connection when the connection gets established.
//
// Arguments:
//
// l ListenArgs:
//   - Determines what protocol to listen on and where to listen, e.g. tcp port 11211 or unix domain socket /tmp/foo.sock
//
// ps []protocol.Components
//   - Used to determine what protocol to use on the connection. The protocols' disambiguators are used *in order*, so
//     protocols that are first will get priority if multiple can parse the same connection.
//
// s server.ServerConst
//   - Used to create the server.Server instance as needed when the connection is established.
//
// o orcas.OrcaConst
//   - Used to create the orcas.Orca instance as needed when the connection is established.
//
// h1, h2 handlers.HandlerConst
//   - Used to create the handlers.Handler instances as needed when the connection is established.
func ListenAndServe(l ListenConst, ps []protocol.Components, s ServerConst, o orcas.OrcaConst, h1, h2 handlers.HandlerConst) {
	listener, err := l()
	if err != nil {
		// At this point the server would be useless since we can't talk to the outside world.
		panic(err)
	}

	for {
		remote, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection from remote:", err.Error())
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedExt)

		err = listener.ModifyConnSettings(remote)
		if err != nil {
			log.Println("Error modifying connection settings after accept:", err.Error())
			remote.Close()
			continue
		}

		// construct L1 handler using given constructor
		l1, err := h1()
		if err != nil {
			log.Println("Error opening connection to L1:", err.Error())
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedL1)

		// construct l2
		l2, err := h2()
		if err != nil {
			log.Println("Error opening connection to L2:", err.Error())
			l1.Close()
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedL2)

		// spin off a goroutine here to handle determining the protocol used for the connection.
		// The server loop can't be started until the protocol is known. Another goroutine is
		// necessary here because we don't want to block accepting new connections if the current
		// new connection doesn't send data immediately.
		go func(remoteConn net.Conn) {
			remoteReader := bufio.NewReader(remoteConn)
			remoteWriter := bufio.NewWriter(remoteConn)

			var reqParser protocol.RequestParser
			var responder protocol.Responder
			var matched bool

			peeker := protocol.Peeker(remoteReader)

			for _, p := range ps {
				match, err := p.NewDisambiguator(peeker).CanParse()

				if err != nil {
					abort([]io.Closer{remoteConn, l1, l2}, err)
					if err == io.EOF {
						metrics.IncCounter(MetricProtocolsAssignedErrorEOF)
					} else {
						metrics.IncCounter(MetricProtocolsAssignedError)
					}
					return
				}

				if match {
					reqParser = p.NewRequestParser(remoteReader)
					responder = p.NewResponder(remoteWriter)
					matched = true
				}
			}

			// if none of the protocols matched, just use the last one in the list
			if !matched {
				p := ps[len(ps)-1]
				reqParser = p.NewRequestParser(remoteReader)
				responder = p.NewResponder(remoteWriter)
				metrics.IncCounter(MetricProtocolsAssignedFallback)
			}

			metrics.IncCounter(MetricProtocolsAssigned)

			server := s([]io.Closer{remoteConn, l1, l2}, reqParser, o(l1, l2, responder))

			go server.Loop()
		}(remote)
	}
}
