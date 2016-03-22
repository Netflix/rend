package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/textprot"
)

type ListenArgs struct {
	Port      int
	L1sock    string
	L2enabled bool
	L2sock    string
}

func ListenAndServe(l ListenArgs, s ServerConst, o orcas.OrcaConst, h1, h2 handlers.HandlerConst) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", l.Port))
	if err != nil {
		log.Printf("Error binding to port %d\n", l.Port)
		return
	}

	for {
		remote, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection from remote:", err.Error())
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedExt)

		tcpRemote := remote.(*net.TCPConn)
		tcpRemote.SetKeepAlive(true)
		tcpRemote.SetKeepAlivePeriod(30 * time.Second)

		l1conn, err := net.Dial("unix", l.L1sock)
		if err != nil {
			log.Println("Error opening connection to L1:", err.Error())
			if l1conn != nil {
				l1conn.Close()
			}
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedL1)

		// construct L1 handler using given constructor
		l1 := h1(l1conn)

		// construct l2 if needed
		var l2 handlers.Handler
		if l.L2enabled {
			l2conn, err := net.Dial("unix", l.L2sock)
			if err != nil {
				log.Println("Error opening connection to L2:", err.Error())
				if l2conn != nil {
					l2conn.Close()
				}
				l1conn.Close()
				remote.Close()
				continue
			}
			metrics.IncCounter(MetricConnectionsEstablishedL2)

			l2 = h2(l2conn)
		}

		// spin off a goroutine here to handle determining the protocol used for the connection.
		// The server loop can't be started until the protocol is known. Another goroutine is
		// necessary here because we don't want to block accepting new connections if the current
		// new connection doesn't send data immediately.
		go func(remoteConn net.Conn) {
			remoteReader := bufio.NewReader(remoteConn)
			remoteWriter := bufio.NewWriter(remoteConn)

			var reqParser common.RequestParser
			var responder common.Responder

			// A connection is either binary protocol or text. It cannot switch between the two.
			// This is the way memcached handles protocols, so it can be as strict here.
			binary, err := isBinaryRequest(remoteReader)
			if err != nil {
				// must be an IO error. Abort!
				abort([]io.Closer{remoteConn, l1, l2}, err)
				return
			}

			if binary {
				reqParser = binprot.NewBinaryParser(remoteReader)
				responder = binprot.NewBinaryResponder(remoteWriter)
			} else {
				reqParser = textprot.NewTextParser(remoteReader)
				responder = textprot.NewTextResponder(remoteWriter)
			}

			server := s([]io.Closer{remoteConn, l1, l2}, reqParser, o(l1, l2, responder))

			go server.Loop()
		}(remote)
	}
}
