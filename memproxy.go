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
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/textprot"
)

// Set GOGC default explicitly
func init() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}
}

// Setting up signal handlers
func init() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		panic("Keyboard Interrupt")
	}()
}

// Set up http debug and metrics endpoint
func init() {
	go http.ListenAndServe("localhost:11299", nil)
}

// Set up the metrics
func init() {
	metrics.SetPrefix("rend_")
}

var (
	MetricConnectionsEstablishedExt = metrics.AddCounter("conn_established_ext")
	MetricConnectionsEstablishedL1  = metrics.AddCounter("conn_established_l1")
	MetricConnectionsEstablishedL2  = metrics.AddCounter("conn_established_l2")
	MetricCmdTotal                  = metrics.AddCounter("cmd_total")
	MetricCmdGet                    = metrics.AddCounter("cmd_get")
	MetricCmdGetL1                  = metrics.AddCounter("cmd_get_l1")
	MetricCmdGetL2                  = metrics.AddCounter("cmd_get_l2")
	MetricCmdGetHits                = metrics.AddCounter("cmd_get_hits")
	MetricCmdGetHitsL1              = metrics.AddCounter("cmd_get_hits_l1")
	MetricCmdGetHitsL2              = metrics.AddCounter("cmd_get_hits_l2")
	MetricCmdGetMisses              = metrics.AddCounter("cmd_get_misses")
	MetricCmdGetMissesL1            = metrics.AddCounter("cmd_get_misses_l1")
	MetricCmdGetMissesL2            = metrics.AddCounter("cmd_get_misses_l2")
	MetricCmdGetErrors              = metrics.AddCounter("cmd_get_errors")
	MetricCmdGetErrorsL1            = metrics.AddCounter("cmd_get_errors_l1")
	MetricCmdGetErrorsL2            = metrics.AddCounter("cmd_get_errors_l2")
	MetricCmdGetKeys                = metrics.AddCounter("cmd_get_keys")
	MetricCmdGetKeysL1              = metrics.AddCounter("cmd_get_keys_l1")
	MetricCmdGetKeysL2              = metrics.AddCounter("cmd_get_keys_l2")
	MetricCmdSet                    = metrics.AddCounter("cmd_set")
	MetricCmdSetL1                  = metrics.AddCounter("cmd_set_l1")
	MetricCmdSetL2                  = metrics.AddCounter("cmd_set_l2")
	MetricCmdSetSuccess             = metrics.AddCounter("cmd_set_success")
	MetricCmdSetSuccessL1           = metrics.AddCounter("cmd_set_success_l1")
	MetricCmdSetSuccessL2           = metrics.AddCounter("cmd_set_success_l2")
	MetricCmdSetErrors              = metrics.AddCounter("cmd_set_errors")
	MetricCmdSetErrorsL1            = metrics.AddCounter("cmd_set_errors_l1")
	MetricCmdSetErrorsL2            = metrics.AddCounter("cmd_set_errors_l2")
	MetricCmdAdd                    = metrics.AddCounter("cmd_add")
	MetricCmdAddL1                  = metrics.AddCounter("cmd_add_l1")
	MetricCmdAddL2                  = metrics.AddCounter("cmd_add_l2")
	MetricCmdAddStored              = metrics.AddCounter("cmd_add_stored")
	MetricCmdAddStoredL1            = metrics.AddCounter("cmd_add_stored_l1")
	MetricCmdAddStoredL2            = metrics.AddCounter("cmd_add_stored_l2")
	MetricCmdAddNotStored           = metrics.AddCounter("cmd_add_not_stored")
	MetricCmdAddNotStoredL1         = metrics.AddCounter("cmd_add_not_stored_l1")
	MetricCmdAddNotStoredL2         = metrics.AddCounter("cmd_add_not_stored_l2")
	MetricCmdAddErrors              = metrics.AddCounter("cmd_add_errors")
	MetricCmdAddErrorsL1            = metrics.AddCounter("cmd_add_errors_l1")
	MetricCmdAddErrorsL2            = metrics.AddCounter("cmd_add_errors_l2")
	MetricCmdReplace                = metrics.AddCounter("cmd_replace")
	MetricCmdReplaceL1              = metrics.AddCounter("cmd_replace_l1")
	MetricCmdReplaceL2              = metrics.AddCounter("cmd_replace_l2")
	MetricCmdReplaceStored          = metrics.AddCounter("cmd_replace_stored")
	MetricCmdReplaceStoredL1        = metrics.AddCounter("cmd_replace_stored_l1")
	MetricCmdReplaceStoredL2        = metrics.AddCounter("cmd_replace_stored_l2")
	MetricCmdReplaceNotStored       = metrics.AddCounter("cmd_replace_not_stored")
	MetricCmdReplaceNotStoredL1     = metrics.AddCounter("cmd_replace_not_stored_l1")
	MetricCmdReplaceNotStoredL2     = metrics.AddCounter("cmd_replace_not_stored_l2")
	MetricCmdReplaceErrors          = metrics.AddCounter("cmd_replace_errors")
	MetricCmdReplaceErrorsL1        = metrics.AddCounter("cmd_replace_errors_l1")
	MetricCmdReplaceErrorsL2        = metrics.AddCounter("cmd_replace_errors_l2")
	MetricCmdDelete                 = metrics.AddCounter("cmd_delete")
	MetricCmdDeleteL1               = metrics.AddCounter("cmd_delete_l1")
	MetricCmdDeleteL2               = metrics.AddCounter("cmd_delete_l2")
	MetricCmdDeleteHits             = metrics.AddCounter("cmd_delete_hits")
	MetricCmdDeleteHitsL1           = metrics.AddCounter("cmd_delete_hits_l1")
	MetricCmdDeleteHitsL2           = metrics.AddCounter("cmd_delete_hits_l2")
	MetricCmdDeleteMisses           = metrics.AddCounter("cmd_delete_misses")
	MetricCmdDeleteMissesL1         = metrics.AddCounter("cmd_delete_misses_l1")
	MetricCmdDeleteMissesL2         = metrics.AddCounter("cmd_delete_misses_l2")
	MetricCmdDeleteErrors           = metrics.AddCounter("cmd_delete_errors")
	MetricCmdDeleteErrorsL1         = metrics.AddCounter("cmd_delete_errors_l1")
	MetricCmdDeleteErrorsL2         = metrics.AddCounter("cmd_delete_errors_l2")
	MetricCmdTouch                  = metrics.AddCounter("cmd_touch")
	MetricCmdTouchL1                = metrics.AddCounter("cmd_touch_l1")
	MetricCmdTouchL2                = metrics.AddCounter("cmd_touch_l2")
	MetricCmdTouchHits              = metrics.AddCounter("cmd_touch_hits")
	MetricCmdTouchHitsL1            = metrics.AddCounter("cmd_touch_hits_l1")
	MetricCmdTouchHitsL2            = metrics.AddCounter("cmd_touch_hits_l2")
	MetricCmdTouchMisses            = metrics.AddCounter("cmd_touch_misses")
	MetricCmdTouchMissesL1          = metrics.AddCounter("cmd_touch_misses_l1")
	MetricCmdTouchMissesL2          = metrics.AddCounter("cmd_touch_misses_l2")
	MetricCmdTouchErrors            = metrics.AddCounter("cmd_touch_errors")
	MetricCmdTouchErrorsL1          = metrics.AddCounter("cmd_touch_errors_l1")
	MetricCmdTouchErrorsL2          = metrics.AddCounter("cmd_touch_errors_l2")
	MetricCmdGat                    = metrics.AddCounter("cmd_gat")
	MetricCmdGatL1                  = metrics.AddCounter("cmd_gat_l1")
	MetricCmdGatL2                  = metrics.AddCounter("cmd_gat_l2")
	MetricCmdGatHits                = metrics.AddCounter("cmd_gat_hits")
	MetricCmdGatHitsL1              = metrics.AddCounter("cmd_gat_hits_l1")
	MetricCmdGatHitsL2              = metrics.AddCounter("cmd_gat_hits_l2")
	MetricCmdGatMisses              = metrics.AddCounter("cmd_gat_misses")
	MetricCmdGatMissesL1            = metrics.AddCounter("cmd_gat_misses_l1")
	MetricCmdGatMissesL2            = metrics.AddCounter("cmd_gat_misses_l2")
	MetricCmdGatErrors              = metrics.AddCounter("cmd_gat_errors")
	MetricCmdGatErrorsL1            = metrics.AddCounter("cmd_gat_errors_l1")
	MetricCmdGatErrorsL2            = metrics.AddCounter("cmd_gat_errors_l2")
	MetricCmdUnknown                = metrics.AddCounter("cmd_unknown")
	MetricCmdQuit                   = metrics.AddCounter("cmd_quit")
	MetricCmdVersion                = metrics.AddCounter("cmd_version")
	MetricErrAppError               = metrics.AddCounter("err_app_err")
	MetricErrUnrecoverable          = metrics.AddCounter("err_unrecoverable")

	HistSet     = metrics.AddHistogram("set")
	HistAdd     = metrics.AddHistogram("add")
	HistReplace = metrics.AddHistogram("replace")
	HistDelete  = metrics.AddHistogram("delete")
	HistTouch   = metrics.AddHistogram("touch")
	HistGet     = metrics.AddHistogram("get")
	HistGat     = metrics.AddHistogram("gat")

	// TODO: inconsistency metrics for when L1 is not a subset of L2
)

// Flags
var chunked bool
var l1sock string
var l2enabled bool
var l2sock string
var port int

func init() {
	flag.BoolVar(&chunked, "chunked", false, "If --chunked is specified, the chunked handler is used for L1")
	flag.StringVar(&l1sock, "l1-sock", "invalid.sock", "Specifies the unix socket to connect to L1")
	flag.BoolVar(&l2enabled, "l2-enabled", false, "Specifies if l2 is enabled")
	flag.StringVar(&l2sock, "l2-sock", "invalid.sock", "Specifies the unix socket to connect to L2. Only used if --l2-enabled is true.")
	flag.IntVar(&port, "p", 11211, "External port to listen on")
	flag.Parse()
}

// And away we go
func main() {
	server, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("Error binding to port %d\n", port)
		return
	}

	for {
		remote, err := server.Accept()
		if err != nil {
			log.Println("Error accepting connection from remote:", err.Error())
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedExt)

		tcpRemote := remote.(*net.TCPConn)
		tcpRemote.SetKeepAlive(true)
		tcpRemote.SetKeepAlivePeriod(30 * time.Second)

		l1conn, err := net.Dial("unix", l1sock)
		if err != nil {
			log.Println("Error opening connection to L1:", err.Error())
			if l1conn != nil {
				l1conn.Close()
			}
			remote.Close()
			continue
		}
		metrics.IncCounter(MetricConnectionsEstablishedL1)

		var l1 handlers.Handler
		if chunked {
			l1 = memcached.NewChunkedHandler(l1conn)
		} else {
			l1 = memcached.NewHandler(l1conn)
		}

		var l2 handlers.Handler

		if l2enabled {
			l2conn, err := net.Dial("unix", l2sock)
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

			l2 = memcached.NewHandler(l2conn)
		}

		go handleConnection(remote, l1, l2)
	}
}

func abort(toClose []io.Closer, err error) {
	if err != nil && err != io.EOF {
		log.Println("Error while processing request. Closing connection. Error:", err.Error())
	}
	for _, c := range toClose {
		if c != nil {
			c.Close()
		}
	}
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
				log.Println("Recovered from runtime panic:", r)
				log.Println("Panic location: ", identifyPanic())
			}
		}
	}()

	remoteReader := bufio.NewReader(remoteConn)
	remoteWriter := bufio.NewWriter(remoteConn)

	var reqParser common.RequestParser
	var responder common.Responder
	var reqType common.RequestType
	var request common.Request

	// A connection is either binary protocol or text. It cannot switch between the two.
	// This is the way memcached handles protocols, so it can be as strict here.
	binary, err := isBinaryRequest(remoteReader)
	if err != nil {
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

	for {
		start := time.Now()

		request, reqType, err = reqParser.Parse()
		if err != nil {
			if err == common.ErrBadRequest ||
				err == common.ErrBadLength ||
				err == common.ErrBadFlags ||
				err == common.ErrBadExptime {
				responder.Error(0, common.RequestUnknown, err)
				continue
			} else {
				abort([]io.Closer{remoteConn, l1, l2}, err)
				return
			}
		}

		metrics.IncCounter(MetricCmdTotal)

		// TODO: handle nil
		switch reqType {
		case common.RequestSet:
			err = handleSet(request, l1, l2, responder)

		case common.RequestAdd:
			err = handleAdd(request, l1, l2, responder)

		case common.RequestReplace:
			err = handleReplace(request, l1, l2, responder)

		case common.RequestDelete:
			err = handleDelete(request, l1, l2, responder)

		case common.RequestTouch:
			err = handleTouch(request, l1, l2, responder)

		case common.RequestGet:
			err = handleGet(request, l1, l2, responder)

		case common.RequestGat:
			err = handleGat(request, l1, l2, responder)

		case common.RequestQuit:
			handleQuit(request, l1, l2, responder)
			abort([]io.Closer{remoteConn, l1, l2}, err)
			return

		case common.RequestVersion:
			err = handleVersion(request, l1, l2, responder)

		case common.RequestUnknown:
			err = handleUnknown(request, l1, l2, responder)
		}

		// TODO: distinguish fatal errors from non-fatal
		if err != nil {
			if common.IsAppError(err) {
				if err != common.ErrKeyNotFound {
					metrics.IncCounter(MetricErrAppError)
				}
				responder.Error(request.Opq(), reqType, err)
			} else {
				metrics.IncCounter(MetricErrUnrecoverable)
				abort([]io.Closer{remoteConn, l1, l2}, err)
				return
			}
		}

		dur := uint64(time.Since(start))
		switch reqType {
		case common.RequestSet:
			metrics.ObserveHist(HistSet, dur)
		case common.RequestAdd:
			metrics.ObserveHist(HistAdd, dur)
		case common.RequestReplace:
			metrics.ObserveHist(HistReplace, dur)
		case common.RequestDelete:
			metrics.ObserveHist(HistDelete, dur)
		case common.RequestTouch:
			metrics.ObserveHist(HistTouch, dur)
		case common.RequestGet:
			metrics.ObserveHist(HistGet, dur)
		case common.RequestGat:
			metrics.ObserveHist(HistGat, dur)
		}
	}
}

func handleSet(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdSet)
	req := request.(common.SetRequest)
	//log.Println("set", string(req.Key))

	metrics.IncCounter(MetricCmdSetL1)
	err := l1.Set(req)

	if err == nil {
		metrics.IncCounter(MetricCmdSetSuccessL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdSetSuccess)

		err = responder.Set(req.Opaque, req.Quiet)

	} else {
		metrics.IncCounter(MetricCmdSetErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdSetErrors)
	}

	// TODO: L2 metrics for sets, set success, set errors

	return err
}

func handleAdd(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdAdd)
	req := request.(common.SetRequest)
	//log.Println("add", string(req.Key))

	// TODO: L2 first, then L1

	metrics.IncCounter(MetricCmdAddL1)
	err := l1.Add(req)

	if err == nil {
		metrics.IncCounter(MetricCmdAddStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdAddStored)

		err = responder.Add(req.Opaque, true, req.Quiet)

	} else if err == common.ErrKeyExists {
		metrics.IncCounter(MetricCmdAddNotStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdAddNotStored)
		err = responder.Add(req.Opaque, false, req.Quiet)
	} else {
		metrics.IncCounter(MetricCmdAddErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdAddErrors)
	}

	// TODO: L2 metrics for adds, add stored, add not stored, add errors

	return err
}

func handleReplace(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdReplace)
	req := request.(common.SetRequest)
	//log.Println("replace", string(req.Key))

	// TODO: L2 first, then L1

	metrics.IncCounter(MetricCmdReplaceL1)
	err := l1.Replace(req)

	if err == nil {
		metrics.IncCounter(MetricCmdReplaceStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdReplaceStored)

		err = responder.Replace(req.Opaque, true, req.Quiet)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(MetricCmdReplaceNotStoredL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdReplaceNotStored)
		err = responder.Replace(req.Opaque, false, req.Quiet)
	} else {
		metrics.IncCounter(MetricCmdReplaceErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdReplaceErrors)
	}

	// TODO: L2 metrics for replaces, replace stored, replace not stored, replace errors

	return err
}

func handleDelete(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdDelete)
	req := request.(common.DeleteRequest)
	//log.Println("delete", string(req.Key))

	metrics.IncCounter(MetricCmdDeleteL1)
	err := l1.Delete(req)

	if err == nil {
		metrics.IncCounter(MetricCmdDeleteHits)
		metrics.IncCounter(MetricCmdDeleteHitsL1)

		responder.Delete(req.Opaque)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(MetricCmdDeleteMissesL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdDeleteMisses)
	} else {
		metrics.IncCounter(MetricCmdDeleteErrorsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdDeleteErrors)
	}

	// TODO: L2 metrics for deletes, delete hits, delete misses, delete errors

	return err
}

func handleTouch(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdTouch)
	req := request.(common.TouchRequest)
	//log.Println("touch", string(req.Key))

	metrics.IncCounter(MetricCmdTouchL1)
	err := l1.Touch(req)

	if err == nil {
		metrics.IncCounter(MetricCmdTouchHitsL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdTouchHits)

		responder.Touch(req.Opaque)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(MetricCmdTouchMissesL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdTouchMisses)
	} else {
		metrics.IncCounter(MetricCmdTouchMissesL1)
		// TODO: Account for L2
		metrics.IncCounter(MetricCmdTouchMisses)
	}

	// TODO: L2 metrics for touches, touch hits, touch misses, touch errors

	return err
}

func handleGet(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdGet)
	req := request.(common.GetRequest)
	metrics.IncCounterBy(MetricCmdGetKeys, uint64(len(req.Keys)))
	//debugString := "get"
	//for _, k := range req.Keys {
	//	debugString += " "
	//	debugString += string(k)
	//}
	//println(debugString)

	metrics.IncCounter(MetricCmdGetL1)
	metrics.IncCounterBy(MetricCmdGetKeysL1, uint64(len(req.Keys)))
	resChan, errChan := l1.Get(req)

	var err error

	// note to self later: gather misses from L1 into a slice and send as gets to L2 in a batch
	// The L2 handler will be able to send it in a batch to L2, which will internally parallelize

	// Read all the responses back from L1.
	// The contract is that the resChan will have GetResponse's for get hits and misses,
	// and the errChan will have any other errors, such as an out of memory error from
	// memcached. If any receive happens from errChan, there will be no more responses
	// from resChan.
	for {
		select {
		case res, ok := <-resChan:
			if !ok {
				resChan = nil
			} else {
				if res.Miss {
					metrics.IncCounter(MetricCmdGetMissesL1)
					// TODO: Account for L2
					metrics.IncCounter(MetricCmdGetMisses)
				} else {
					metrics.IncCounter(MetricCmdGetHits)
					metrics.IncCounter(MetricCmdGetHitsL1)
				}
				responder.Get(res)
			}

		case getErr, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				metrics.IncCounter(MetricCmdGetErrors)
				metrics.IncCounter(MetricCmdGetErrorsL1)
				err = getErr
			}
		}

		if resChan == nil && errChan == nil {
			break
		}
	}

	if err == nil {
		responder.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	// TODO: L2 metrics for gets, get hits, get misses, get errors

	return err
}

func handleGat(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdGat)
	req := request.(common.GATRequest)
	//log.Println("gat", string(req.Key))

	metrics.IncCounter(MetricCmdGatL1)
	res, err := l1.GAT(req)

	if err == nil {
		if res.Miss {
			metrics.IncCounter(MetricCmdGatMissesL1)
			// TODO: Account for L2
			metrics.IncCounter(MetricCmdGatMisses)
		} else {
			metrics.IncCounter(MetricCmdGatHits)
			metrics.IncCounter(MetricCmdGatHitsL1)
		}
		responder.GAT(res)
		// There is no GetEnd call required here since this is only ever
		// done in the binary protocol, where there's no END marker.
		// Calling responder.GetEnd was a no-op here and is just useless.
		//responder.GetEnd(0, false)
	} else {
		metrics.IncCounter(MetricCmdGatErrors)
		metrics.IncCounter(MetricCmdGatErrorsL1)
	}

	//TODO: L2 metrics for gats, gat hits, gat misses, gat errors

	return err
}

func handleQuit(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdQuit)
	req := request.(common.QuitRequest)
	return responder.Quit(req.Opaque, req.Quiet)
}

func handleVersion(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdVersion)
	req := request.(common.VersionRequest)
	return responder.Version(req.Opaque)
}

func handleUnknown(request common.Request, l1, l2 handlers.Handler, responder common.Responder) error {
	metrics.IncCounter(MetricCmdUnknown)
	return common.ErrUnknownCmd
}

func isBinaryRequest(reader *bufio.Reader) (bool, error) {
	headerByte, err := reader.Peek(1)
	if err != nil {
		return false, err
	}
	return headerByte[0] == binprot.MagicRequest, nil
}
